package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

public interface HydrationService<T extends RecordDto> {

    RecordTypeEnum getRecordType();

    String getDeltaQuery();

    T hydrateDomainData(T domainData);

    Pair<String, Iterable<GetQueryResultsResponse>> fetchDataFromAthena(String query);

    List<T> mapRowsToDomainData(String[] columnNames, List<Row> rows);

    void saveHydrationFailedRecord(HydrationStep step, String recordId, String errorMessage);

}
