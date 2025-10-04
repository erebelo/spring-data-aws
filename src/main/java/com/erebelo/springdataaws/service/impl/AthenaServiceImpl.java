package com.erebelo.springdataaws.service.impl;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import com.erebelo.springdataaws.service.AthenaService;
import com.erebelo.springdataaws.util.ObjectMapperUtil;
import com.fasterxml.jackson.databind.JavaType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

@Slf4j
@RequiredArgsConstructor
public class AthenaServiceImpl implements AthenaService {

    private final AthenaClient athenaClient;
    private final String athenaDatabase;
    private final String outputBucketUrl;
    private final String workgroup;

    @Override
    public AthenaQueryDto submitAthenaQuery(String queryString) {
        try {
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder().database(athenaDatabase)
                    .build();
            ResultConfiguration resultConfiguration = ResultConfiguration.builder().outputLocation(outputBucketUrl)
                    .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(queryString).queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration).workGroup(workgroup).build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient
                    .startQueryExecution(startQueryExecutionRequest);

            if (startQueryExecutionResponse == null || startQueryExecutionResponse.queryExecutionId() == null
                    || startQueryExecutionResponse.queryExecutionId().isEmpty()) {
                throw new AthenaQueryException("Failed to execute Athena query: No execution Id returned");
            }

            return AthenaQueryDto.builder().executionId(startQueryExecutionResponse.queryExecutionId()).build();
        } catch (AthenaException e) {
            log.info("Failed to execute Athena query: {}", e.getMessage());
            throw new AthenaQueryException("Failed to execute Athena query", e);
        }
    }

    @Override
    public void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;

        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();

            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                String errorCause = getQueryExecutionResponse.queryExecution().status().stateChangeReason();
                log.error("The Athena query failed to run: {}", errorCause);
                throw new AthenaQueryException("The Athena query failed to run: " + errorCause);
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                log.error("The Athena query was cancelled");
                throw new AthenaQueryException("The Athena query was cancelled");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(300);
            }
            log.info("The current status of the query is: {}", queryState);
        }
    }

    @Override
    public Iterable<GetQueryResultsResponse> getQueryResults(String queryExecutionId) {
        try {
            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId).build();

            return athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
        } catch (AthenaException e) {
            log.info("Failed to get query results: {}", e.getMessage());
            throw new AthenaQueryException("Failed to get query results", e);
        }
    }

    @Override
    public List<String> getQueryResultsAsStrings(String queryExecutionId) {
        try {
            GetQueryResultsIterable getQueryResultsIterable = (GetQueryResultsIterable) getQueryResults(
                    queryExecutionId);

            List<String> queryResults = new ArrayList<>();
            for (GetQueryResultsResponse result : getQueryResultsIterable) {
                List<Row> results = result.resultSet().rows();

                results.forEach(row -> {
                    List<Datum> allData = row.data();
                    allData.forEach(data -> queryResults.add(data.varCharValue()));
                });
            }

            return queryResults;
        } catch (AthenaQueryException e) {
            log.info(e.getMessage());
            throw new AthenaQueryException(e.getMessage(), e);
        }
    }

    /*
     * Maps Athena Row objects to a list of instances of the given class, based on
     * the provided order of column names returned by Athena.
     */
    public <T> List<T> mapRowsToClass(String[] athenaColumnOrder, List<Row> rows, Class<T> clazz) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> normalizedColumns = Arrays.stream(athenaColumnOrder).toList();
        List<T> result = new ArrayList<>(rows.size());

        Map<String, Field> fieldTypes = Arrays.stream(clazz.getDeclaredFields())
                .collect(Collectors.toMap(Field::getName, f -> f));

        for (Row row : rows) {
            List<Datum> allData = row.data();
            Map<String, Object> fieldMap = new LinkedHashMap<>();

            for (int i = 0; i < normalizedColumns.size(); i++) {
                String key = normalizedColumns.get(i);
                String rawValue = (i < allData.size() && allData.get(i) != null) ? allData.get(i).varCharValue() : null;

                Field field = fieldTypes.get(key);
                if (field != null && rawValue != null) {
                    JavaType javaType = ObjectMapperUtil.objectMapper.getTypeFactory()
                            .constructType(field.getGenericType());

                    Object converted = ObjectMapperUtil.objectMapper.convertValue(rawValue, javaType);
                    fieldMap.put(key, converted);
                } else {
                    fieldMap.put(key, rawValue);
                }
            }

            T instance = ObjectMapperUtil.objectMapper.convertValue(fieldMap, clazz);
            result.add(instance);
        }

        return result;
    }
}
