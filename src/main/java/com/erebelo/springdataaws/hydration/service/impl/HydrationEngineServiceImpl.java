package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;
import com.erebelo.springdataaws.hydration.service.HydrationEngineService;
import com.erebelo.springdataaws.hydration.service.HydrationJobService;
import com.erebelo.springdataaws.hydration.service.HydrationService;
import com.erebelo.springdataaws.hydration.service.HydrationStepService;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

@Slf4j
@Service
@RequiredArgsConstructor
public class HydrationEngineServiceImpl implements HydrationEngineService {

    private final List<HydrationService<? extends RecordDto>> hydrationPipeline;
    private final HydrationJobService hydrationJobService;
    private final HydrationStepService hydrationStepService;
    private final ApplicationContext applicationContext;
    private final Executor asyncTaskExecutor;

    /*
     * Returns the Spring proxy to ensure @Transactional methods are applied when
     * called internally.
     */
    private HydrationEngineServiceImpl getSelfProxy() {
        return applicationContext.getBean(HydrationEngineServiceImpl.class);
    }

    /**
     * Main trigger method. Can be called programmatically with specific record
     * types.
     */
    @Override
    public String triggerHydration(RecordTypeEnum... recordTypes) {
        HydrationJob job = initJobIfNoneRunning();
        if (job != null) {
            Map<String, String> loggingContext = MDC.getCopyOfContextMap(); // Capture the current logging context

            CompletableFuture.runAsync(() -> {
                if (loggingContext != null) {
                    // Restore the logging context in the asynchronous task
                    MDC.setContextMap(loggingContext);
                }
                try {
                    log.info("Hydration triggered");
                    executeJob(job, recordTypes);
                } finally {
                    // Clear the logging context after the task completes
                    MDC.clear();
                }
            }, asyncTaskExecutor);

            return job.getId();
        }

        return hydrationJobService.getCurrentJob().getId();
    }

    @Override
    public HydrationJob initJobIfNoneRunning() {
        if (!hydrationJobService.existsInitiatedOrProcessingJob()) {
            return hydrationJobService.initNewJob();
        }
        return null;
    }

    @Override
    public void executeJob(HydrationJob job, RecordTypeEnum... recordTypes) {
        log.info("Starting to execute job: {}", job.getId());
        List<RecordTypeEnum> filteringTypes = Arrays.asList(recordTypes);
        List<HydrationService<? extends RecordDto>> servicesToRun = ObjectUtils.isEmpty(filteringTypes)
                ? hydrationPipeline
                : hydrationPipeline.stream().filter(s -> filteringTypes.contains(s.getRecordType())).toList();

        for (HydrationService<? extends RecordDto> service : servicesToRun) {
            HydrationStep step = hydrationStepService.initNewStep(service.getRecordType(), job.getId());

            try {
                getSelfProxy().fetchAndHydrate(service, job, step);
                hydrationStepService.updateStepStatus(step, HydrationStatus.COMPLETED);
            } catch (Exception e) {
                log.error("Error occurred while processing job: {}", job.getId(), e);
                hydrationStepService.updateStepStatus(step, HydrationStatus.FAILED);
                hydrationJobService.updateJobStatus(job, HydrationStatus.FAILED);
                // Abort processing remaining services in the pipeline due to failure
                return;
            }
        }

        hydrationJobService.updateJobStatus(job, HydrationStatus.COMPLETED);
    }

    @Transactional
    public void fetchAndHydrate(HydrationService<? extends RecordDto> service, HydrationJob job, HydrationStep step) {
        Pair<String, Iterable<GetQueryResultsResponse>> responses = service
                .fetchDataFromAthena(service.getDeltaQuery());

        step.setExecutionId(responses.getLeft());
        hydrationStepService.updateStepStatus(step, HydrationStatus.PROCESSING);
        hydrationJobService.updateJobStatus(job, HydrationStatus.PROCESSING);

        AtomicBoolean headerProcessed = new AtomicBoolean(false);
        AtomicReference<String[]> athenaColumnOrder = new AtomicReference<>();

        Iterator<GetQueryResultsResponse> iterator = responses.getRight().iterator();
        iterator.forEachRemaining(response -> {
            List<Row> rows = response.resultSet().rows();
            if (rows == null || rows.isEmpty()) {
                return;
            }

            // On first batch, extract header and adjust rows
            rows = processAndSkipHeaderOnce(rows, headerProcessed, athenaColumnOrder);

            if (!rows.isEmpty()) {
                hydrateDomainData(service, step, athenaColumnOrder.get(), rows);
            }
        });
    }

    private <T extends RecordDto> void hydrateDomainData(HydrationService<T> service, HydrationStep step,
            String[] athenaColumnOrder, List<Row> rows) {
        List<T> dataRecords = service.mapRowsToDomainData(athenaColumnOrder, rows);

        for (T dataRecord : dataRecords) {
            try {
                T hydratedRecord = service.hydrateDomainData(dataRecord);
                hydratedRecord.setRecordId(dataRecord.getRecordId());
            } catch (Exception e) {
                service.saveHydrationFailedRecord(step, dataRecord.getRecordId(), e.getMessage());
                throw new IllegalStateException(String.format(
                        "An error occurred while processing and hydrating %s record. RecordId: %s Error: %s",
                        dataRecord.getClass().getSimpleName(), dataRecord.getRecordId(), e.getMessage()));
            }
        }
    }

    private List<Row> processAndSkipHeaderOnce(List<Row> rows, AtomicBoolean headerProcessed,
            AtomicReference<String[]> athenaColumnOrder) {
        if (!headerProcessed.getAndSet(true)) {
            Row headerRow = rows.getFirst();
            athenaColumnOrder.set(headerRow.data().stream().map(Datum::varCharValue).toArray(String[]::new));

            return rows.size() > 1 ? rows.subList(1, rows.size()) : Collections.emptyList();
        }
        return rows;
    }
}
