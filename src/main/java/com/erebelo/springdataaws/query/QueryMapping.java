package com.erebelo.springdataaws.query;

import static com.erebelo.springdataaws.constant.AddressConstant.ADDRESS_QUERY_NAME;
import static com.erebelo.springdataaws.query.AddressQueryTemplate.LEGACY_ADDRESSES_QUERY_TEMPLATE;

import jakarta.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.PropertyPlaceholderHelper;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "athena")
public class QueryMapping {

    private String database;
    private String legacyAddresses;

    private Map<String, String> queryTemplate = new HashMap<>();
    private Map<String, Map<String, String>> queryTableProperties = new HashMap<>();

    private final PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    @PostConstruct
    public void init() {
        queryTemplate.put(ADDRESS_QUERY_NAME, LEGACY_ADDRESSES_QUERY_TEMPLATE);
        queryTemplate = Collections.unmodifiableMap(queryTemplate);

        queryTableProperties.put(ADDRESS_QUERY_NAME, Map.of("db", database, "legacyAddresses", legacyAddresses));
        queryTableProperties = Collections.unmodifiableMap(queryTableProperties);
    }

    public String getQueryByName(String queryName) {
        return placeholderHelper.replacePlaceholders(queryTemplate.get(queryName),
                queryTableProperties.get(queryName)::get);
    }
}
