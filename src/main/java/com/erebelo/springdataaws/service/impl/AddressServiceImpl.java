package com.erebelo.springdataaws.service.impl;

import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_QUERY_NAME;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_S3_CONTENT_TYPE;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_S3_KEY_NAME;
import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_S3_METADATA_TITLE;

import com.erebelo.springdataaws.dto.address.AddressBundleDto;
import com.erebelo.springdataaws.dto.address.AddressContextDto;
import com.erebelo.springdataaws.dto.address.AddressDto;
import com.erebelo.springdataaws.dto.athena.AthenaQueryDto;
import com.erebelo.springdataaws.exception.BadRequestException;
import com.erebelo.springdataaws.query.QueryMapping;
import com.erebelo.springdataaws.service.AddressService;
import com.erebelo.springdataaws.service.AthenaService;
import com.erebelo.springdataaws.service.S3Service;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

@Slf4j
@Service
@RequiredArgsConstructor
public class AddressServiceImpl implements AddressService {

    private final AthenaService athenaService;
    private final S3Service s3Service;
    private final Executor asyncTaskExecutor;

    private static final List<String> ADDRESS_FIELD_NAMES = Arrays.stream(AddressDto.class.getDeclaredFields())
            .map(field -> field.getName().toLowerCase()).toList();
    private static final List<String> ADDRESS_BUNDLE_FIELD_NAMES = Arrays
            .stream(AddressBundleDto.class.getDeclaredFields()).map(field -> field.getName().toLowerCase()).toList();

    public AthenaQueryDto addressTrigger() {
        log.info("Triggering the addresses table in Athena");
        AddressContextDto context = new AddressContextDto(null, 0, new ByteArrayOutputStream());

        // Capture the current logging context
        Map<String, String> loggingContext = MDC.getCopyOfContextMap();

        try {
            String query = QueryMapping.getQueryByName(ADDRESS_QUERY_NAME);
            context.setExecutionId(athenaService.submitAthenaQuery(query).getExecutionId());
            log.info("Executing join query among address feed tables. Execution ID='{}'", context.getExecutionId());

            athenaService.waitForQueryToComplete(context.getExecutionId());
            log.info("Query execution completed");

            log.info("Fetching address query results");
            Iterable<GetQueryResultsResponse> paginatedResults = athenaService
                    .getQueryResults(context.getExecutionId());

            log.info("Starting to process address records");
            CompletableFuture.runAsync(() -> {
                // Restore the logging context in the asynchronous task
                if (loggingContext != null) {
                    MDC.setContextMap(loggingContext);
                }
                try {
                    processResults(paginatedResults, context);
                } finally {
                    // Clear the logging context after the task completes
                    MDC.clear();
                }
            }, asyncTaskExecutor);

            return AthenaQueryDto.builder().executionId(context.getExecutionId()).build();
        } catch (Exception e) {
            throw new BadRequestException(extractAndLogError("Failed to trigger address feed.", e, context), e);
        }
    }

    public void processResults(Iterable<GetQueryResultsResponse> paginatedResults, AddressContextDto context) {
        long startTime = System.nanoTime();

        // Process initial results synchronously to include csv headers in the first row
        Iterator<GetQueryResultsResponse> iterator = paginatedResults.iterator();
        if (iterator.hasNext()) {
            processAndWriteRows(iterator.next().resultSet().rows(), false, context);
        }

        // Process all remaining results asynchronously
        while (iterator.hasNext()) {
            processAndWriteRows(iterator.next().resultSet().rows(), true, context);
        }

        log.info("Uploading in-memory address csv file to S3 bucket");
        uploadFileToS3(context);

        long duration = System.nanoTime() - startTime;
        log.info("{} address records successfully processed in {} seconds", context.getProcessedRecords(),
                duration / 1_000_000_000.0);
    }

    private void processAndWriteRows(List<Row> rows, boolean isAsynchronous, AddressContextDto context) {
        if (rows != null && !rows.isEmpty()) {
            List<Map<String, String>> addressMapList = isAsynchronous
                    ? processRowsAsynchronously(rows, context)
                    : processRowsSynchronously(rows, context);

            if (!addressMapList.isEmpty()) {
                writeAddressesToCsv(addressMapList, context);
                log.info("Processed {} address record(s)", context.getProcessedRecords());
            }
        }
    }

    private List<Map<String, String>> processRowsSynchronously(List<Row> rows, AddressContextDto context) {
        // Directly process rows with csv headers in the first row
        return rows.stream().map(row -> buildAddressMapFromRow(row, context)) // Exclude empty maps (failed rows)
                .filter(addressMap -> !addressMap.isEmpty()).toList();
    }

    private List<Map<String, String>> processRowsAsynchronously(List<Row> rows, AddressContextDto context) {
        // Exclude empty maps (failed rows)
        return rows.parallelStream().map(row -> buildAddressMapFromRow(row, context))
                .filter(addressMap -> !addressMap.isEmpty()).toList();
    }

    private Map<String, String> buildAddressMapFromRow(Row row, AddressContextDto context) {
        try {
            List<Datum> allData = row.data();

            // Dynamically creates a bundle address map object from Athena results
            Map<String, String> bundleAddressMap = parseDataToBundleAddressMap(allData);

            // Dynamically builds an address map object from bundle address map object
            return buildAddressMap(bundleAddressMap);
        } catch (Exception e) {
            String recordId = "UNKNOWN";

            try {
                // Extract recordId from the row
                Datum recordIdDatum = row.data().getFirst();
                recordId = recordIdDatum.varCharValue() != null ? recordIdDatum.varCharValue().trim() : "UNKNOWN";
            } catch (Exception ex) {
                log.error("Failed to extract recordId from row: " + row, ex);
            }

            extractAndLogError("Error processing row (skipping it) with recordId=" + recordId + ".", e, context);
            return new LinkedHashMap<>();
        }
    }

    private Map<String, String> parseDataToBundleAddressMap(List<Datum> allData) {
        Map<String, String> bundleAddressMap = new LinkedHashMap<>();

        for (int i = 0; i < ADDRESS_BUNDLE_FIELD_NAMES.size(); i++) {
            String key = ADDRESS_BUNDLE_FIELD_NAMES.get(i);
            String value = (i < allData.size() && allData.get(i) != null && allData.get(i).varCharValue() != null)
                    ? allData.get(i).varCharValue().trim()
                    : null;
            bundleAddressMap.put(key, value);
        }

        return bundleAddressMap;
    }

    private Map<String, String> buildAddressMap(Map<String, String> bundleAddressMap) {
        Map<String, String> addressMap = new LinkedHashMap<>();

        for (String fieldName : ADDRESS_FIELD_NAMES) {
            addressMap.put(fieldName, bundleAddressMap.get(fieldName));
        }

        return addressMap;
    }

    private void writeAddressesToCsv(List<Map<String, String>> addressMapList, AddressContextDto context) {
        // Collect all csv rows
        StringBuilder csvContent = new StringBuilder();
        for (Map<String, String> addressMap : addressMapList) {
            csvContent.append(convertMapToCsvRow(addressMap)).append("\n");
        }

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(context.getByteArrayOutputStream()))) {
            writer.write(csvContent.toString());
            context.setProcessedRecords(context.getProcessedRecords() + addressMapList.size());
        } catch (IOException e) {
            throw new BadRequestException(extractAndLogError("Failed to write address data to file.", e, context), e);
        }
    }

    private String convertMapToCsvRow(Map<String, String> elementMap) {
        return elementMap.values().stream().map(value -> value != null ? value : "").collect(Collectors.joining(","));
    }

    private void uploadFileToS3(AddressContextDto context) {
        try {
            byte[] fileBytes = context.getByteArrayOutputStream().toByteArray();
            s3Service.multipartUpload(ADDRESS_S3_KEY_NAME, ADDRESS_S3_METADATA_TITLE, ADDRESS_S3_CONTENT_TYPE,
                    fileBytes);
        } catch (Exception e) {
            throw new BadRequestException(
                    extractAndLogError("Failed to upload in-memory address file to S3.", e, context), e);
        }
    }

    private String extractAndLogError(String errorMsg, Exception e, AddressContextDto context) {
        errorMsg = errorMsg + " Execution ID='" + context.getExecutionId() + "' Error Cause='"
                + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()) + "'";
        log.error(errorMsg);
        return errorMsg;
    }
}
