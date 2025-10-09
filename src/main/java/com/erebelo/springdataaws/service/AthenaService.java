package com.erebelo.springdataaws.service;

import com.erebelo.springdataaws.domain.dto.AthenaContextDto;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

public interface AthenaService {

    Pair<String, Iterable<GetQueryResultsResponse>> fetchDataFromAthena(String query);

    <T extends AthenaContextDto> List<Row> processAndSkipHeaderOnce(List<Row> rows, T context);

    <T> List<T> mapRowsToClass(String[] athenaColumnOrder, List<Row> rows, Class<T> clazz);

}
