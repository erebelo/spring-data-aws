package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.model.HydrationFailedRecord;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;
import com.erebelo.springdataaws.hydration.repository.HydrationFailedRecordRepository;
import com.erebelo.springdataaws.service.AthenaService;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

public abstract class AbstractHydrationService<T extends RecordDto> implements HydrationService<T> {

    protected HydrationJobService hydrationJobService;
    private AthenaService athenaService;
    private HydrationFailedRecordRepository hydrationFailedRecordRepository;
    private final Class<T> clazz;

    protected AbstractHydrationService(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Autowired
    public final void setHydrationJobService(HydrationJobService hydrationJobService) {
        this.hydrationJobService = hydrationJobService;
    }

    @Autowired
    @Qualifier("hydrationAthenaService")
    public final void setAthenaService(AthenaService athenaService) {
        this.athenaService = athenaService;
    }

    @Autowired
    public final void setHydrationFailedRecordRepository(
            HydrationFailedRecordRepository hydrationFailedRecordRepository) {
        this.hydrationFailedRecordRepository = hydrationFailedRecordRepository;
    }

    @Override
    public Pair<String, Iterable<GetQueryResultsResponse>> fetchDataFromAthena(String query) {
        AthenaQueryDto athenaQuery = athenaService.submitAthenaQuery(query);
        String executionId = athenaQuery.getExecutionId();

        try {
            athenaService.waitForQueryToComplete(executionId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted while waiting for Athena query to complete", e);
        }

        return Pair.of(executionId, athenaService.getQueryResults(executionId));
    }

    @Override
    public List<T> mapRowsToDomainData(String[] athenaColumnOrder, List<Row> rows) {
        return athenaService.mapRowsToClass(athenaColumnOrder, rows, this.clazz);
    }

    /*
     * Executes in a separate transaction to ensure failure logs are persisted even
     * if the main transaction rolls back.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveHydrationFailedRecord(HydrationStep step, String recordId, String errorMessage) {
        HydrationFailedRecord failedRecord = HydrationFailedRecord.builder().recordId(recordId).stepId(step.getId())
                .executionId(step.getExecutionId()).domainType(this.getRecordType()).errorMessage(errorMessage).build();
        hydrationFailedRecordRepository.save(failedRecord);
    }
}
