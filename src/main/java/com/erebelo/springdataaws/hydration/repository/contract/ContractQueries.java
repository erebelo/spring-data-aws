package com.erebelo.springdataaws.hydration.repository.contract;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.PropertyPlaceholderHelper;

import java.util.Map;

@Component
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "athena.hydration")
public class ContractQueries {

    private final String database;
    private final String advisorContract;
    private final String firmContract;

    private static final String ADVISOR_CONTRACT_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS recordId, *
               FROM ${db}.${advisor}
               WHERE hydrationRun = ${runNumber}
            """;

    private static final String FIRM_CONTRACT_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS recordId, *
               FROM ${db}.${firm}
               WHERE hydrationRun = ${runNumber}
            """;

    private final PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    private Map<String, String> buildHydrationTables(Long runNumber) {
        return Map.of(
                "db", database,
                "advisor", advisorContract,
                "firm", firmContract,
                "runNumber", String.valueOf(runNumber)
        );
    }

    public String getAdvisorContractDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(ADVISOR_CONTRACT_QUERY_TEMPLATE, buildHydrationTables(runNumber)::get);
    }

    public String getFirmContractDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(FIRM_CONTRACT_QUERY_TEMPLATE, buildHydrationTables(runNumber)::get);
    }
}
