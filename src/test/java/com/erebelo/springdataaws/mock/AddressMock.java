package com.erebelo.springdataaws.mock;

import com.erebelo.springdataaws.domain.dto.AddressDto;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.ResultSet;
import software.amazon.awssdk.services.athena.model.Row;

@UtilityClass
public class AddressMock {

    public static final String ADDRESSES_QUERY_TEMPLATE = "SELECT * FROM test_table";
    public static final String EXECUTION_ID = "3e0135ac-d582-4cb2-b671-f74c945d13e2";

    public static Pair<String, Iterable<GetQueryResultsResponse>> getResponsePair() {
        Iterable<GetQueryResultsResponse> results = List.of(
                GetQueryResultsResponse.builder().resultSet(ResultSet.builder().rows(getRowsChunk1()).build()).build(),
                GetQueryResultsResponse.builder().resultSet(ResultSet.builder().rows(getRowsChunk2()).build()).build());
        return Pair.of(EXECUTION_ID, results);
    }

    public static List<Row> getRowsChunk1() {
        List<Row> rows = new ArrayList<>();
        rows.addFirst(getHeaderRow());
        rows.addAll(getRows(999));
        return rows;
    }

    public static List<Row> getRowsChunk2() {
        return getRows(1000);
    }

    private static List<Row> getRows(int end) {
        return IntStream.range(0, end).mapToObj(i -> getRow()).toList();
    }

    private static Row getRow() {
        return Row.builder().data(List.of(Datum.builder().varCharValue("1").build(),
                Datum.builder().varCharValue("ADDRID3473").build(), Datum.builder().varCharValue("HOME").build(),
                Datum.builder().varCharValue("123 Main St").build(), Datum.builder().varCharValue("Apt 4B").build(),
                Datum.builder().varCharValue("New York").build(), Datum.builder().varCharValue("NY").build(),
                Datum.builder().varCharValue("US").build(), Datum.builder().varCharValue("10001").build(),
                Datum.builder().varCharValue("WORK").build(), Datum.builder().varCharValue("456 Business Rd").build(),
                Datum.builder().varCharValue("Suite 200").build(),
                Datum.builder().varCharValue("San Francisco").build(), Datum.builder().varCharValue("CA").build(),
                Datum.builder().varCharValue("US").build(), Datum.builder().varCharValue("94105").build(),
                Datum.builder().varCharValue("OTHER").build(), Datum.builder().varCharValue("789 Other Ave").build(),
                Datum.builder().varCharValue("Unit 5").build(), Datum.builder().varCharValue("Chicago").build(),
                Datum.builder().varCharValue("IL").build(), Datum.builder().varCharValue("US").build(),
                Datum.builder().varCharValue(null).build() // keep it null for test coverage
        )).build();
    }

    private static Row getHeaderRow() {
        return Row.builder()
                .data(Arrays.stream(AddressDto.class.getDeclaredFields()).map(field -> field.getName().toLowerCase())
                        .map(fieldName -> Datum.builder().varCharValue(fieldName).build()).toList())
                .build();
    }
}
