package com.erebelo.springdataaws.query;

import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_QUERY_NAME;
import static com.erebelo.springdataaws.query.AddressQuery.ADDRESS_QUERY;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
public class QueryMapping {

    private static final Map<String, String> QUERIES;

    static {
        Map<String, String> map = new HashMap<>();
        map.put(ADDRESS_QUERY_NAME, ADDRESS_QUERY);

        QUERIES = Collections.unmodifiableMap(map);
    }

    public static String getQueryByName(String queryName) {
        return QUERIES.get(queryName);
    }
}
