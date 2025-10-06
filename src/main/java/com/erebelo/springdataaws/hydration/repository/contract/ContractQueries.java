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
               SELECT ROW_NUMBER() OVER () AS record_id, *
               FROM ${db}.${advisorContracts}
               WHERE run_number = '${runNumber}';
            """;

    private static final String FIRM_CONTRACTS_QUERY_TEMPLATE = """
               SELECT ROW_NUMBER() OVER () AS record_id, *
               FROM ${db}.${firmContracts}
               WHERE run_number = '${runNumber}';
            """;

    private final PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    private Map<String, String> buildHydrationContractTables(Long runNumber) {
        return Map.of("db", database, "advisorContracts", advisorContracts, "firmContracts", firmContracts, "runNumber",
                String.valueOf(runNumber));
    }

    public String getAdvisorContractsDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(ADVISOR_CONTRACTS_QUERY_TEMPLATE,
                buildHydrationContractTables(runNumber)::get);
    }

    public String getFirmContractsDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(FIRM_CONTRACTS_QUERY_TEMPLATE,
                buildHydrationContractTables(runNumber)::get);
    }
}
