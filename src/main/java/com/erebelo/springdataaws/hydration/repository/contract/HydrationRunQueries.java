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
public class HydrationRunQueries {

    private String database;
    private String hydrationRuns;

    private static final String HYDRATION_RUNS_QUERY_TEMPLATE = """
               SELECT *
               FROM ${db}.${hydrationRuns}
               WHERE run_number = '${runNumber}'
               LIMIT 1;
            """;

    private final PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    private Map<String, String> buildHydrationRunTables(Long runNumber) {
        return Map.of("db", database, "hydrationRuns", hydrationRuns, "runNumber", String.valueOf(runNumber));
    }

    public String getHydrationRunsDataQuery(Long runNumber) {
        return placeholderHelper.replacePlaceholders(HYDRATION_RUNS_QUERY_TEMPLATE,
                buildHydrationRunTables(runNumber)::get);
    }
}
