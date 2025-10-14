package com.erebelo.springdataaws.mock;

import java.util.ArrayList;
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

    public static final String LEGACY_ADDRESSES_QUERY_TEMPLATE = "SELECT * FROM test_table";
    public static final String EXECUTION_ID = "3e0135ac-d582-4cb2-b671-f74c945d13e2";

    public static Pair<String, Iterable<GetQueryResultsResponse>> getResponsePair() {
        Iterable<GetQueryResultsResponse> results = List.of(
                GetQueryResultsResponse.builder().resultSet(ResultSet.builder().rows(getRowsChunk1()).build()).build(),
                GetQueryResultsResponse.builder().resultSet(ResultSet.builder().rows(getRowsChunk2()).build()).build());
        return Pair.of(EXECUTION_ID, results);
    }

    private static List<Row> getRowsChunk1() {
        List<Row> rows = new ArrayList<>();
        rows.addFirst(getHeaderRow());
        rows.addAll(getRows(999));
        return rows;
    }

    private static List<Row> getRowsChunk2() {
        return getRows(1000);
    }

    private static List<Row> getRows(int end) {
        return IntStream.range(0, end).mapToObj(i -> getRow(i + 1)).toList();
    }

    private static Row getRow(int recordId) {
        return Row.builder().data(List.of(Datum.builder().varCharValue(String.valueOf(recordId)).build(), // record_id
                Datum.builder().varCharValue("CI2318").build(), // address_id
                Datum.builder().varCharValue("123 Main St").build(), // home_address_line_1
                Datum.builder().varCharValue("Apt 4B").build(), // home_address_line_2
                Datum.builder().varCharValue("New York").build(), // home_city
                Datum.builder().varCharValue("NY").build(), // home_state
                Datum.builder().varCharValue("96573").build(), // home_zip_code
                Datum.builder().varCharValue("US").build(), // home_country
                Datum.builder().varCharValue("456 Business Rd").build(), // work_address_line_1
                Datum.builder().varCharValue("Suite 200").build(), // work_address_line_2
                Datum.builder().varCharValue("San Francisco").build(), // work_city
                Datum.builder().varCharValue("CA").build(), // work_state
                Datum.builder().varCharValue("94105").build(), // work_zip_code
                Datum.builder().varCharValue("US").build(), // work_country
                Datum.builder().varCharValue("789 Other Ave").build(), // other_address_line_1
                Datum.builder().varCharValue("Unit 5").build(), // other_address_line_2
                Datum.builder().varCharValue("Chicago").build(), // other_city
                Datum.builder().varCharValue("IL").build(), // other_state
                Datum.builder().varCharValue(null).build(), // other_zip_code (null for test coverage)
                Datum.builder().varCharValue("US").build() // other_state
        )).build();
    }

    private static Row getHeaderRow() {
        return Row.builder().data(List.of(Datum.builder().varCharValue("record_id").build(),
                Datum.builder().varCharValue("address_id").build(),
                Datum.builder().varCharValue("home_address_line_1").build(),
                Datum.builder().varCharValue("home_address_line_2").build(),
                Datum.builder().varCharValue("home_city").build(), Datum.builder().varCharValue("home_state").build(),
                Datum.builder().varCharValue("home_zip_code").build(),
                Datum.builder().varCharValue("home_country").build(),
                Datum.builder().varCharValue("work_address_line_1").build(),
                Datum.builder().varCharValue("work_address_line_2").build(),
                Datum.builder().varCharValue("work_city").build(), Datum.builder().varCharValue("work_state").build(),
                Datum.builder().varCharValue("work_zip_code").build(),
                Datum.builder().varCharValue("work_country").build(),
                Datum.builder().varCharValue("other_address_line_1").build(),
                Datum.builder().varCharValue("other_address_line_2").build(),
                Datum.builder().varCharValue("other_city").build(), Datum.builder().varCharValue("other_state").build(),
                Datum.builder().varCharValue("other_zip_code").build(),
                Datum.builder().varCharValue("other_country").build())).build();
    }
}
