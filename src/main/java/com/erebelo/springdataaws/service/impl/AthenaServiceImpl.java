package com.erebelo.springdataaws.service.impl;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import com.erebelo.springdataaws.service.AthenaService;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
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
@Service
@RequiredArgsConstructor
public class AthenaServiceImpl implements AthenaService {

    private final AthenaClient athenaClient;

    @Value("${athena.database.name}")
    private String athenaDBName;

    @Value("${s3.output.bucket.url}")
    private String outputBucketUrl;

    @Override
    public AthenaQueryDto submitAthenaQuery(String queryString) {
        try {
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder().database(athenaDBName)
                    .build();
            ResultConfiguration resultConfiguration = ResultConfiguration.builder().outputLocation(outputBucketUrl)
                    .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(queryString).queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration).build();

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
}
