package com.erebelo.springdataaws.service;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import java.util.List;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

public interface AthenaService {

    AthenaQueryDto submitAthenaQuery(String queryString);

    void waitForQueryToComplete(String queryExecutionId) throws InterruptedException;

    Iterable<GetQueryResultsResponse> getQueryResults(String queryExecutionId);

    List<String> getQueryResultsAsStrings(String queryExecutionId);

    <T> List<T> mapRowsToClass(String[] columnNames, List<Row> rows, Class<T> clazz);

}
