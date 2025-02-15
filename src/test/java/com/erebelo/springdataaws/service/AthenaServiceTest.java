package com.erebelo.springdataaws.service;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;
import software.amazon.awssdk.services.athena.model.ResultSet;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

@ExtendWith(MockitoExtension.class)
class AthenaServiceTest {

    @InjectMocks
    private AthenaServiceImpl service;

    @Mock
    private AthenaClient athenaClient;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "athenaDBName", "db_test");
        ReflectionTestUtils.setField(service, "outputBucketUrl", "s3://test-output-bucket");
    }

    @Test
    void testSubmitAthenaQuery() {
        StartQueryExecutionResponse mockResponse = StartQueryExecutionResponse.builder().queryExecutionId("12345")
                .build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(mockResponse);

        AthenaQueryDto athenaQueryDto = service.submitAthenaQuery("SELECT * FROM acct_v5");

        assertEquals("12345", athenaQueryDto.getExecutionId());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
    }

    @Test
    void testSubmitAthenaQueryThrowsException() {
        String queryString = "SELECT * FROM test_table";
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class)))
                .willThrow(AthenaException.builder().message("Test exception").build());

        assertThatExceptionOfType(AthenaQueryException.class).isThrownBy(() -> service.submitAthenaQuery(queryString))
                .withMessage("Failed to execute Athena query");

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
    }

    @Test
    void submitAthenaQueryThrowsExceptionWhenResponseIsNull() {
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(null);

        assertThatExceptionOfType(AthenaQueryException.class)
                .isThrownBy(() -> service.submitAthenaQuery("SELECT * FROM test_table"))
                .withMessage("Failed to execute Athena query: No execution Id returned");

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
    }

    @Test
    void testWaitForQueryToComplete() throws InterruptedException {
        String queryExecutionId = "12345";
        GetQueryExecutionResponse getQueryExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build()).build())
                .build();
        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class)))
                .willReturn(getQueryExecutionResponse);

        service.waitForQueryToComplete(queryExecutionId);

        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void testWaitForQueryToCompleteThrowsException() {
        String queryExecutionId = "12345";
        GetQueryExecutionResponse getQueryExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder()
                        .state(QueryExecutionState.FAILED).stateChangeReason("Test failure reason").build()).build())
                .build();
        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class)))
                .willReturn(getQueryExecutionResponse);

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> service.waitForQueryToComplete(queryExecutionId))
                .withMessage("The Athena query failed to run: Test failure reason");

        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void waitForQueryToCompleteCancelledState() {
        String queryExecutionId = "12345";
        GetQueryExecutionResponse getQueryExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder()
                        .state(QueryExecutionState.CANCELLED).stateChangeReason("Test cancellation reason").build())
                        .build())
                .build();

        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class)))
                .willReturn(getQueryExecutionResponse);

        assertThatExceptionOfType(AthenaQueryException.class)
                .isThrownBy(() -> service.waitForQueryToComplete(queryExecutionId))
                .withMessage("The Athena query was cancelled");

        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void waitForQueryToCompleteRetriesOnRunningState() throws InterruptedException {
        String queryExecutionId = "12345";
        GetQueryExecutionResponse runningResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.RUNNING).build()).build())
                .build();
        GetQueryExecutionResponse succeededResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build()).build())
                .build();

        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willReturn(runningResponse)
                .willReturn(succeededResponse);

        service.waitForQueryToComplete(queryExecutionId);

        verify(athenaClient, atLeast(2)).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void testGetQueryResults() {
        String queryExecutionId = "12345";
        GetQueryResultsIterable getQueryResultsIterable = mock(GetQueryResultsIterable.class);
        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class)))
                .willReturn(getQueryResultsIterable);

        Iterable<GetQueryResultsResponse> response = service.getQueryResults(queryExecutionId);

        assertEquals(getQueryResultsIterable, response);

        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testGetQueryResultsThrowsException() {
        String queryExecutionId = "12345";
        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class)))
                .willThrow(AthenaException.builder().message("Test exception").build());

        assertThatExceptionOfType(AthenaQueryException.class)
                .isThrownBy(() -> service.getQueryResults(queryExecutionId)).withMessage("Failed to get query results");

        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testGetQueryResultsAsStringsSuccess() {
        String queryExecutionId = "test-query-id";

        Datum datum = Datum.builder().varCharValue("data1").build();
        Row row = Row.builder().data(datum).build();
        List<Row> rows = new ArrayList<>();
        rows.add(row);

        GetQueryResultsResponse response = GetQueryResultsResponse.builder()
                .resultSet(ResultSet.builder().rows(rows).build()).build();
        GetQueryResultsIterable iterable = mock(GetQueryResultsIterable.class);
        given(iterable.iterator()).willReturn(List.of(response).iterator());

        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class))).willReturn(iterable);

        List<String> results = service.getQueryResultsAsStrings(queryExecutionId);

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("data1", results.getFirst());
    }

    @Test
    void testGetQueryResultsAsStringsAthenaException() {
        String queryExecutionId = "test-query-id";
        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class)))
                .willThrow(AthenaException.class);

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.getQueryResultsAsStrings(queryExecutionId));

        assertEquals("Failed to get query results", exception.getMessage());
    }

    @Test
    void testGetQueryResultsAsStringsThrowsException() {
        String queryExecutionId = "12345";
        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class)))
                .willThrow(AthenaException.builder().message("Test exception").build());

        assertThatExceptionOfType(AthenaQueryException.class)
                .isThrownBy(() -> service.getQueryResultsAsStrings(queryExecutionId))
                .withMessage("Failed to get query results");

        assertThatExceptionOfType(AthenaQueryException.class)
                .isThrownBy(() -> service.getQueryResultsAsStrings(queryExecutionId))
                .withMessage("Failed to get query results2");


        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testGetQueryResultsAsStringsQueryThrowsException() {
        String queryExecutionId = "12345";
        given(service.getQueryResults(queryExecutionId))
                .willThrow(new AthenaQueryException("Failed to execute Athena query"));

        assertThatExceptionOfType(AthenaQueryException.class)
                .isThrownBy(() -> service.getQueryResultsAsStrings(queryExecutionId))
                .withMessage("Failed to execute Athena query");
    }
}
