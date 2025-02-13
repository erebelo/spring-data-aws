package com.erebelo.springdataaws.service;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import java.util.List;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;

public interface AthenaService {

    AthenaQueryDto submitAthenaQuery(String queryString);

    void waitForQueryToComplete(String queryExecutionId);

    Iterable<GetQueryResultsResponse> getQueryResults(String queryExecutionId);

    List<String> getQueryResultsAsStrings(String queryExecutionId);

}
