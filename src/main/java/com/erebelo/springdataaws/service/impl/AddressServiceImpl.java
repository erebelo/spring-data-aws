package com.erebelo.springdataaws.service.impl;

import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_ATHENA_BATCH_SIZE;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_QUERY_NAME;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_S3_CONTENT_TYPE;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_S3_KEY_NAME;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_S3_METADATA_TITLE;

import com.erebelo.springdataaws.domain.dto.AddressContextDto;
import com.erebelo.springdataaws.domain.dto.AddressDto;
import com.erebelo.springdataaws.query.QueryMapping;
import com.erebelo.springdataaws.service.AddressService;
import com.erebelo.springdataaws.service.AthenaService;
import com.erebelo.springdataaws.service.S3Service;
import com.erebelo.springdataaws.util.ObjectMapperUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

@Slf4j
@Service
public class AddressServiceImpl implements AddressService {

    private final QueryMapping queryMapping;
    private final AthenaService athenaService;
    private final S3Service s3Service;
    private final Executor asyncTaskExecutor;

    public AddressServiceImpl(QueryMapping queryMapping, AthenaService athenaService, S3Service s3Service,
            @Qualifier("asyncTaskExecutor") Executor asyncTaskExecutor) {
        this.queryMapping = queryMapping;
        this.athenaService = athenaService;
        this.s3Service = s3Service;
        this.asyncTaskExecutor = asyncTaskExecutor;
    }

    @Override
    public String triggerAddressFeed() {
        log.info("Triggering the addresses table feed in Athena");
        Pair<String, Iterable<GetQueryResultsResponse>> responsePair = athenaService
                .fetchDataFromAthena(queryMapping.getQueryByName(ADDRESS_QUERY_NAME));

        AddressContextDto context = AddressContextDto.builder().headerProcessed(false).athenaColumnOrder(null)
                .executionId(responsePair.getLeft()).headerWritten(false).processedRecords(0)
                .byteArrayOutputStream(new ByteArrayOutputStream()).build();
        Map<String, String> loggingContext = MDC.getCopyOfContextMap();
        log.info("Processing query results to feed addresses tables. Execution ID='{}'", context.getExecutionId());

        CompletableFuture.runAsync(() -> {
            if (loggingContext != null) {
                MDC.setContextMap(loggingContext);
            }
            try {
                processResults(responsePair.getRight(), context);
            } finally {
                MDC.clear();
            }
        }, asyncTaskExecutor);

        return context.getExecutionId();
    }

    private void processResults(Iterable<GetQueryResultsResponse> results, AddressContextDto context) {
        log.info("Starting to process address records");
        long startTime = System.nanoTime();
        List<Row> batchRows = new ArrayList<>();

        try {
            Iterator<GetQueryResultsResponse> iterator = results.iterator();
            iterator.forEachRemaining(response -> {
                List<Row> rows = response.resultSet().rows();
                if (rows == null || rows.isEmpty()) {
                    return;
                }

                // On first batch, extract header and adjust rows
                rows = athenaService.processAndSkipHeaderOnce(rows, context);

                if (!rows.isEmpty()) {
                    batchRows.addAll(rows);
                }

                if (batchRows.size() >= (ADDRESS_ATHENA_BATCH_SIZE - 1) || !iterator.hasNext()) {
                    processAndWriteRows(batchRows, context);
                    batchRows.clear();
                }
            });

            log.info("Uploading in-memory address csv file to S3 bucket");
            uploadFileToS3(context);

            long duration = Math.round((System.nanoTime() - startTime) / 1_000_000_000.0);
            log.info("{} address records successfully processed in {} seconds", context.getProcessedRecords(),
                    duration);
        } catch (Exception e) {
            throw new IllegalStateException(extractAndLogError("Failed to trigger address feed", e, context), e);
        }
    }

    private void processAndWriteRows(List<Row> rows, AddressContextDto context) {
        if (!rows.isEmpty()) {
            List<AddressDto> addresses = athenaService.mapRowsToClass(context.getAthenaColumnOrder(), rows,
                    AddressDto.class);

            if (!addresses.isEmpty()) {
                if (!context.isHeaderWritten()) {
                    // Convert column names to a map (key=value) for CSV writer
                    List<Map<String, String>> headerMapList = List.of(Arrays.stream(context.getAthenaColumnOrder())
                            .collect(Collectors.toMap(col -> col, col -> col)));

                    writeMapListToCsv(headerMapList, context);
                    context.setHeaderWritten(true);
                }

                writeMapListToCsv(convertToMapList(addresses), context);
                log.info("Processed {} address record(s)", context.getProcessedRecords());
            }
        }
    }

    private <T> List<Map<String, String>> convertToMapList(List<T> objects) {
        return objects.stream().map(obj -> {
            Map<String, Object> map = ObjectMapperUtil.objectMapper.convertValue(obj, new TypeReference<>() {
            });

            // Convert all values to strings, replacing nulls with empty strings
            return map.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, e -> e.getValue() != null ? e.getValue().toString() : ""));
        }).toList();
    }

    private void writeMapListToCsv(List<Map<String, String>> mapList, AddressContextDto context) {
        StringBuilder csvContent = new StringBuilder();
        for (Map<String, String> map : mapList) {
            csvContent.append(convertMapToCsvRow(map)).append("\n");
        }

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(context.getByteArrayOutputStream()))) {
            writer.write(csvContent.toString());
            context.setProcessedRecords(context.getProcessedRecords() + mapList.size());
        } catch (IOException e) {
            throw new IllegalStateException(extractAndLogError("Failed to write data to file", e, context), e);
        }
    }

    private String convertMapToCsvRow(Map<String, String> elementMap) {
        return elementMap.values().stream().map(value -> value != null ? value : "").collect(Collectors.joining(","));
    }

    private void uploadFileToS3(AddressContextDto context) {
        byte[] fileBytes = context.getByteArrayOutputStream().toByteArray();
        s3Service.multipartUpload(ADDRESS_S3_KEY_NAME, ADDRESS_S3_METADATA_TITLE, ADDRESS_S3_CONTENT_TYPE, fileBytes);
    }

    private String extractAndLogError(String errorMsg, Exception e, AddressContextDto context) {
        errorMsg = "Error: '" + errorMsg + "'. Execution ID: '" + context.getExecutionId() + "'. Root Cause: '"
                + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()) + "'.";
        log.error(errorMsg);
        return errorMsg;
    }
}
