package com.erebelo.springdataaws.hydration.repository.contract;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.PropertyPlaceholderHelper;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "athena.hydration")
public class ContractQueries {

    private String database;
    private String advisorContracts;
    private String firmContracts;

    private static final String ADVISOR_CONTRACTS_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS recordId, *
               FROM ${db}.${advisors}
               WHERE hydrationRun = ${runNumber}
            """;

    private static final String FIRM_CONTRACTS_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS recordId, *
               FROM ${db}.${firms}
               WHERE hydrationRun = ${runNumber}
            """;

    private final PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    private Map<String, String> buildHydrationTables(Long runNumber) {
        return Map.of("db", database, "advisors", advisorContracts, "firms", firmContracts, "runNumber",
                String.valueOf(runNumber));
    }

    public String getAdvisorContractsDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(ADVISOR_CONTRACTS_QUERY_TEMPLATE,
                buildHydrationTables(runNumber)::get);
    }

    public String getFirmContractsDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(FIRM_CONTRACTS_QUERY_TEMPLATE,
                buildHydrationTables(runNumber)::get);
    }
}
