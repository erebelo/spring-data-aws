package com.erebelo.springdataaws.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
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
        ReflectionTestUtils.setField(service, "athenaDatabase", "db_test");
        ReflectionTestUtils.setField(service, "outputBucketUrl", "s3://test-output-bucket");
        ReflectionTestUtils.setField(service, "workgroup", "test_wg");
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

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.submitAthenaQuery(queryString));

        assertEquals("Failed to execute Athena query", exception.getMessage());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
    }

    @Test
    void submitAthenaQueryThrowsExceptionWhenResponseIsNull() {
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(null);

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.submitAthenaQuery("SELECT * FROM test_table"));

        assertEquals("Failed to execute Athena query: No execution Id returned", exception.getMessage());

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

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> service.waitForQueryToComplete(queryExecutionId));

        assertEquals("The Athena query failed to run: Test failure reason", exception.getMessage());

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

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.waitForQueryToComplete(queryExecutionId));

        assertEquals("The Athena query was cancelled", exception.getMessage());

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

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.getQueryResults(queryExecutionId));

        assertEquals("Failed to get query results", exception.getMessage());

        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testGetQueryResultsAsStringsSuccessful() {
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

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.getQueryResultsAsStrings(queryExecutionId));

        assertEquals("Failed to get query results", exception.getMessage());

        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testGetQueryResultsAsStringsQueryThrowsException() {
        String queryExecutionId = "12345";
        given(service.getQueryResults(queryExecutionId))
                .willThrow(new AthenaQueryException("Failed to execute Athena query"));

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.getQueryResultsAsStrings(queryExecutionId));

        assertEquals("Failed to execute Athena query", exception.getMessage());
    }

    @Test
    void testMapRowsToClassWithRandomColumnOrderSuccessful() {
        Row row1 = Row.builder()
                .data(Datum.builder().varCharValue("first").build(), Datum.builder().varCharValue("val1").build(),
                        Datum.builder().varCharValue("10").build(), Datum.builder().varCharValue("2025-10-02").build())
                .build();

        Row row2 = Row.builder()
                .data(Datum.builder().varCharValue("second").build(), Datum.builder().varCharValue("val2").build(),
                        Datum.builder().varCharValue("20").build(), Datum.builder().varCharValue("").build())
                .build();

        List<Row> rows = List.of(row1, row2);

        List<TestEntity> result = service.mapRowsToClass(new String[]{"name", "value", "id", "startDate"}, rows,
                TestEntity.class);

        assertEquals(2, result.size());
        assertEquals("10", result.getFirst().getId());
        assertEquals("first", result.getFirst().getName());
        assertEquals("val1", result.getFirst().getValue());
        assertEquals(LocalDate.of(2025, 10, 2), result.getFirst().getStartDate());
        assertEquals("20", result.getLast().getId());
        assertEquals("second", result.getLast().getName());
        assertEquals("val2", result.getLast().getValue());
        assertNull(result.getLast().getStartDate());
    }

    @Test
    void testMapRowsToClassHandlesNullRows() {
        List<TestEntity> result = service.mapRowsToClass(new String[]{"id", "name", "value", "startDate"}, null,
                TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    void testMapRowsToClassHandlesEmptyRows() {
        List<TestEntity> result = service.mapRowsToClass(new String[]{"id", "name", "value", "startDate"},
                Collections.emptyList(), TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    void testMapRowsToClassHandlesMissingValues() {
        Datum d1 = Datum.builder().varCharValue("123").build();
        Row row = Row.builder().data(d1).build();

        List<TestEntity> result = service.mapRowsToClass(new String[]{"id", "name", "value", "startDate"}, List.of(row),
                TestEntity.class);

        assertEquals(1, result.size());
        TestEntity entity = result.getFirst();
        assertEquals("123", entity.getId());
        assertNull(entity.getName());
        assertNull(entity.getValue());
    }

    @Getter
    @Setter
    static class TestEntity {
        private String id;
        private String name;
        private String value;
        private LocalDate startDate;
    }
}
