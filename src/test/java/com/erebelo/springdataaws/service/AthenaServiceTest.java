package com.erebelo.springdataaws.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.erebelo.springdataaws.domain.dto.AthenaContextDto;
import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
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

    private static final String QUERY = "SELECT * FROM test_table";
    private static final String EXECUTION_ID = "12345";

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "athenaDatabase", "db_test");
        ReflectionTestUtils.setField(service, "outputBucketUrl", "s3://test-output-bucket");
        ReflectionTestUtils.setField(service, "workgroup", "test_wg");
    }

    @Test
    void testFetchDataFromAthenaSuccessful() {
        StartQueryExecutionResponse mockStartResponse = StartQueryExecutionResponse.builder()
                .queryExecutionId(EXECUTION_ID).build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(mockStartResponse);

        GetQueryExecutionResponse mockGetExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build()).build())
                .build();
        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willReturn(mockGetExecutionResponse);

        GetQueryResultsIterable mockResultsIterable = mock(GetQueryResultsIterable.class);
        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class))).willReturn(mockResultsIterable);

        Pair<String, Iterable<GetQueryResultsResponse>> result = service.fetchDataFromAthena(QUERY);

        assertNotNull(result);
        assertEquals(EXECUTION_ID, result.getLeft());
        assertEquals(mockResultsIterable, result.getRight());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void fetchDataFromAthenaThrowsInterruptedExceptionWhenSubmitAthenaQuery() {
        StartQueryExecutionResponse startResponse = StartQueryExecutionResponse.builder().queryExecutionId(EXECUTION_ID)
                .build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(startResponse);

        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willAnswer(invocation -> {
            throw new InterruptedException("Simulated interruption");
        });

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.fetchDataFromAthena(QUERY));

        assertEquals("Thread interrupted while waiting for Athena query to complete", exception.getMessage());
        assertTrue(Thread.currentThread().isInterrupted(),
                "Thread should remain interrupted after catching InterruptedException");

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        verify(athenaClient).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void testFetchDataFromAthenaSubmitQueryFails() {
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class)))
                .willThrow(AthenaException.builder().message("Simulated failure").build());

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.fetchDataFromAthena(QUERY));

        assertEquals("Failed to execute Athena query", exception.getMessage());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
    }

    @Test
    void testFetchDataFromAthenaThrowsAthenaQueryExceptionWhenSubmitAthenaQuery() {
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class)))
                .willReturn(StartQueryExecutionResponse.builder().queryExecutionId("").build());

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.fetchDataFromAthena(QUERY));

        assertEquals("Failed to execute Athena query: No execution Id returned", exception.getMessage());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
    }

    @Test
    void testFetchDataFromAthenaQueryExecutionFails() {
        StartQueryExecutionResponse mockStartResponse = StartQueryExecutionResponse.builder()
                .queryExecutionId(EXECUTION_ID).build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(mockStartResponse);

        GetQueryExecutionResponse failedResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder()
                        .state(QueryExecutionState.FAILED).stateChangeReason("Execution failed").build()).build())
                .build();
        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willReturn(failedResponse);

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.fetchDataFromAthena(QUERY));

        assertEquals("The Athena query failed to run: Execution failed", exception.getMessage());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void testFetchDataFromAthenaQueryCancelled() {
        StartQueryExecutionResponse mockStartResponse = StartQueryExecutionResponse.builder()
                .queryExecutionId(EXECUTION_ID).build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(mockStartResponse);

        GetQueryExecutionResponse cancelledResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder()
                        .state(QueryExecutionState.CANCELLED).stateChangeReason("Cancelled by user").build()).build())
                .build();
        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willReturn(cancelledResponse);

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.fetchDataFromAthena(QUERY));

        assertEquals("The Athena query was cancelled", exception.getMessage());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
    }

    @Test
    void testFetchDataFromAthenaQueryRetriesUntilSuccess() {
        StartQueryExecutionResponse mockStartResponse = StartQueryExecutionResponse.builder().queryExecutionId("12345")
                .build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(mockStartResponse);

        GetQueryExecutionResponse runningResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.RUNNING).build()).build())
                .build();

        GetQueryExecutionResponse successResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build()).build())
                .build();

        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willReturn(runningResponse)
                .willReturn(successResponse);

        GetQueryResultsIterable mockResultsIterable = mock(GetQueryResultsIterable.class);
        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class))).willReturn(mockResultsIterable);

        Pair<String, Iterable<GetQueryResultsResponse>> result = service.fetchDataFromAthena(QUERY);

        assertNotNull(result);
        assertEquals(EXECUTION_ID, result.getLeft());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        verify(athenaClient, atLeast(2)).getQueryExecution(any(GetQueryExecutionRequest.class));
        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testFetchDataFromAthenaThrowsAthenaQueryExceptionWhenGetQueryResults() {
        StartQueryExecutionResponse mockStartResponse = StartQueryExecutionResponse.builder()
                .queryExecutionId(EXECUTION_ID).build();
        given(athenaClient.startQueryExecution(any(StartQueryExecutionRequest.class))).willReturn(mockStartResponse);

        GetQueryExecutionResponse mockGetExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(QueryExecution.builder()
                        .status(QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build()).build())
                .build();
        given(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class))).willReturn(mockGetExecutionResponse);

        given(athenaClient.getQueryResultsPaginator(any(GetQueryResultsRequest.class)))
                .willThrow(AthenaException.builder().message("Simulated failure").build());

        AthenaQueryException exception = assertThrows(AthenaQueryException.class,
                () -> service.fetchDataFromAthena(QUERY));

        assertEquals("Failed to get query results", exception.getMessage());

        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        verify(athenaClient, atLeastOnce()).getQueryExecution(any(GetQueryExecutionRequest.class));
        verify(athenaClient).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void testProcessAndSkipHeaderOnceWhenHeaderNotProcessed() {
        TestContextDto context = new TestContextDto();
        context.setHeaderProcessed(false);

        Row headerRow = Row
                .builder().data(Datum.builder().varCharValue("col1").build(),
                        Datum.builder().varCharValue("col2").build(), Datum.builder().varCharValue("col3").build())
                .build();

        Row dataRow1 = Row.builder().data(Datum.builder().varCharValue("v1").build(),
                Datum.builder().varCharValue("v2").build(), Datum.builder().varCharValue("v3").build()).build();

        List<Row> rows = List.of(headerRow, dataRow1);

        List<Row> result = service.processAndSkipHeaderOnce(rows, context);

        assertTrue(context.isHeaderProcessed(), "Header should be marked as processed");
        assertArrayEquals(new String[]{"col1", "col2", "col3"}, context.getAthenaColumnOrder(),
                "Column order should match header row");
        assertEquals(1, result.size(), "Should skip the header row");
        assertEquals(dataRow1, result.getFirst());
    }

    @Test
    void testProcessAndSkipHeaderOnceWhenHeaderAlreadyProcessed() {
        TestContextDto context = new TestContextDto();
        context.setHeaderProcessed(true);
        context.setAthenaColumnOrder(new String[]{"col1", "col2"});

        Row row1 = Row.builder()
                .data(Datum.builder().varCharValue("a").build(), Datum.builder().varCharValue("b").build()).build();

        List<Row> rows = List.of(row1);

        List<Row> result = service.processAndSkipHeaderOnce(rows, context);

        assertSame(rows, result, "Should return the same list when header already processed");
        assertArrayEquals(new String[]{"col1", "col2"}, context.getAthenaColumnOrder(),
                "Column order should remain unchanged");
    }

    @Test
    void testProcessAndSkipHeaderOnceWithEmptyRows() {
        TestContextDto context = new TestContextDto();
        context.setHeaderProcessed(false);

        List<Row> rows = Collections.emptyList();

        assertThrows(NoSuchElementException.class, () -> service.processAndSkipHeaderOnce(rows, context),
                "Expected NoSuchElementException when rows are empty");
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

        List<TestEntity> result = service.mapRowsToClass(new String[]{"name", "value", "id", "start_date"}, rows,
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
    void testMapRowsToClassWithSuperClassInheritanceSuccessful() {
        Row row = Row.builder()
                .data(Datum.builder().varCharValue("1").build(), Datum.builder().varCharValue("first").build(), // duplicate
                        // name
                        Datum.builder().varCharValue("10").build(), Datum.builder().varCharValue("second").build(),
                        Datum.builder().varCharValue("val1").build(),
                        Datum.builder().varCharValue("2025-10-02").build())
                .build();

        List<TestEntity> result = service.mapRowsToClass(
                new String[]{"record_id", "name", "id", "name", "value", "start_date"}, List.of(row), TestEntity.class);

        assertEquals(1, result.size());
        assertEquals("1", result.getFirst().getRecordId());
        assertEquals("10", result.getFirst().getId());
        assertEquals("second", result.getFirst().getName());
        assertEquals("val1", result.getFirst().getValue());
        assertEquals(LocalDate.of(2025, 10, 2), result.getFirst().getStartDate());
    }

    @Test
    void testMapRowsToClassWithMissingColumnNamesThrowsIllegalArgumentException() {
        Row row = Row.builder()
                .data(Datum.builder().varCharValue("1").build(), Datum.builder().varCharValue("10").build(),
                        Datum.builder().varCharValue("second").build(), Datum.builder().varCharValue("val1").build(),
                        Datum.builder().varCharValue("2025-10" + "-02").build())
                .build();

        List<Row> rows = List.of(row);
        String[] athenaColumnOrder = new String[]{"record_id", "id", "name", "value"};

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.mapRowsToClass(athenaColumnOrder, rows, TestEntity.class));

        assertEquals(String.format("Row has %d values but expected %d columns: %s", row.data().size(),
                athenaColumnOrder.length, Arrays.toString(athenaColumnOrder)), exception.getMessage());
    }

    @Test
    void testMapRowsToClassWithMissingRowDataThrowsIllegalArgumentException() {
        Row row1 = Row.builder()
                .data(Datum.builder().varCharValue("1").build(), Datum.builder().varCharValue("10").build(),
                        Datum.builder().varCharValue("first").build(), Datum.builder().varCharValue("val1").build(),
                        Datum.builder().varCharValue("2025-10" + "-02").build())
                .build();

        Row row2 = Row.builder()
                .data(Datum.builder().varCharValue("2").build(), Datum.builder().varCharValue("20").build(),
                        Datum.builder().varCharValue("second").build(),
                        Datum.builder().varCharValue("2025-10-12").build())
                .build();

        List<Row> rows = List.of(row1, row2);
        String[] athenaColumnOrder = new String[]{"record_id", "id", "name", "value", "start_date"};

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.mapRowsToClass(athenaColumnOrder, rows, TestEntity.class));

        assertEquals(String.format("Row has %d values but expected %d columns: %s", rows.get(1).data().size(),
                athenaColumnOrder.length, Arrays.toString(athenaColumnOrder)), exception.getMessage());
    }

    @Test
    void testMapRowsToClassHandlesNullRows() {
        List<TestEntity> result = service.mapRowsToClass(new String[]{"id", "name", "value", "start_date"}, null,
                TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    void testMapRowsToClassHandlesEmptyRows() {
        List<TestEntity> result = service.mapRowsToClass(new String[]{"id", "name", "value", "start_date"},
                Collections.emptyList(), TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @SuperBuilder
    @NoArgsConstructor
    static class TestContextDto extends AthenaContextDto {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestEntity extends TestSuperEntity {
        private String id;
        private String name;
        private String value;
        @JsonProperty("start_date")
        private LocalDate startDate;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestSuperEntity {
        @JsonProperty("record_id")
        private String recordId;
        private String name; // test duplicate attribute
    }
}
