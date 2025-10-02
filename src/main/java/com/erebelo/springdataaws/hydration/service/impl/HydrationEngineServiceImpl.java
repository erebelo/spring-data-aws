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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

@Slf4j
@Service
@RequiredArgsConstructor
public class HydrationEngineServiceImpl implements HydrationEngineService {

    private final List<HydrationService<? extends RecordDto>> hydrationPipeline;
    private final HydrationJobService hydrationJobService;
    private final HydrationStepService hydrationStepService;
    private final Executor asyncTaskExecutor;

    @Scheduled(fixedRateString = "${hydration.scheduler.fixed-rate}")
    public String triggerHydration(RecordTypeEnum... recordTypes) {
        if (!hydrationJobService.existsInitiatedOrProcessingJob()) {
            HydrationJob job = hydrationJobService.initNewJob();
            Map<String, String> loggingContext = MDC.getCopyOfContextMap(); // Capture the current logging context

            CompletableFuture.runAsync(() -> {
                if (loggingContext != null) {
                    // Restore the logging context in the asynchronous task
                    MDC.setContextMap(loggingContext);
                }
                try {
                    processJob(job, recordTypes);
                } finally {
                    // Clear the logging context after the task completes
                    MDC.clear();
                }
            }, asyncTaskExecutor);

            return job.getId();
        }

        return hydrationJobService.getCurrentJob().getId();
    }

    protected void processJob(HydrationJob job, RecordTypeEnum... recordTypes) {
        List<RecordTypeEnum> filteringTypes = Arrays.asList(recordTypes);
        List<HydrationService<? extends RecordDto>> servicesToRun = ObjectUtils.isEmpty(filteringTypes)
                ? hydrationPipeline
                : hydrationPipeline.stream().filter(s -> filteringTypes.contains(s.getRecordType())).toList();

        for (HydrationService<? extends RecordDto> service : servicesToRun) {
            HydrationStep step = hydrationStepService.initNewStep(service.getRecordType(), job.getId());

            try {
                fetchAndHydrate(service, job, step);
                hydrationStepService.updateStepStatus(step, HydrationStatus.COMPLETED);
            } catch (Exception e) {
                log.error("Error occurred while processing job: {}", job.getId(), e);
                hydrationStepService.updateStepStatus(step, HydrationStatus.FAILED);
                hydrationJobService.updateJobStatus(job, HydrationStatus.FAILED);
                return;
            }
        }

        hydrationJobService.updateJobStatus(job, HydrationStatus.COMPLETED);
    }

    protected void fetchAndHydrate(HydrationService<? extends RecordDto> service, HydrationJob job,
            HydrationStep step) {
        Pair<String, Iterable<GetQueryResultsResponse>> responses = service
                .fetchDataFromAthena(service.getDeltaQuery());
        step.setExecutionId(responses.getLeft());

        hydrationStepService.updateStepStatus(step, HydrationStatus.PROCESSING);
        hydrationJobService.updateJobStatus(job, HydrationStatus.PROCESSING);

        AtomicBoolean headerSkipped = new AtomicBoolean(false);
        Iterator<GetQueryResultsResponse> iterator = responses.getRight().iterator();
        iterator.forEachRemaining(i -> {
            List<Row> rows = skipHeaderOnce(i.resultSet().rows(), headerSkipped);

            if (!rows.isEmpty()) {
                hydrateDomainData(service, rows, step);
            }
        });
    }

    protected <T extends RecordDto> void hydrateDomainData(HydrationService<T> service, List<Row> rows,
            HydrationStep step) {
        List<T> dataRecords = service.mapRowsToDomainData(rows);

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

    private List<Row> skipHeaderOnce(List<Row> rows, AtomicBoolean headerSkipped) {
        if (!headerSkipped.getAndSet(true) && !rows.isEmpty()) {
            return rows.subList(1, rows.size());
        }
        return rows;
    }
}
