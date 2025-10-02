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

    private String databaseName;
    private String advisorContract;
    private String firmContract;

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
        return Map.of("db", databaseName, "advisor", advisorContract, "firm", firmContract, "runNumber",
                String.valueOf(runNumber));
    }

    public String getAdvisorContractDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(ADVISOR_CONTRACT_QUERY_TEMPLATE,
                buildHydrationTables(runNumber)::get);
    }

    public String getFirmContractDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(FIRM_CONTRACT_QUERY_TEMPLATE,
                buildHydrationTables(runNumber)::get);
    }
}
