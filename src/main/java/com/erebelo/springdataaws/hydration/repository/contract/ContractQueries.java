package com.erebelo.springdataaws.hydration.repository.contract;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.PropertyPlaceholderHelper;

@Component
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "athena.hydration")
public class ContractQueries {

    private final String database;
    private final String advisorContract;
    private final String firmContract;

    private static final String ADVISOR_CONTRACT_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS recordId, *
               FROM "${db}".${advisor}
            """;

    private static final String FIRM_CONTRACT_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS recordId, *
               FROM "${db}".${firm}
            """;

    private final PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    private Map<String, String> hydrationTables = new HashMap<>();

    @PostConstruct
    public void init() {
        hydrationTables = Map.of("db", database, "advisor", advisorContract, "firm", firmContract);
    }

    public String getAdvisorContractDataQuery() {
        return placeholderHelper.replacePlaceholders(ADVISOR_CONTRACT_QUERY_TEMPLATE, hydrationTables::get);
    }

    public String getFirmContractDataQuery() {
        return placeholderHelper.replacePlaceholders(FIRM_CONTRACT_QUERY_TEMPLATE, hydrationTables::get);
    }
}
