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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

@Slf4j
@Service
public class HydrationEngineServiceImpl implements HydrationEngineService {

    // Returns the Spring proxy to enable @Transactional on internal calls.
    // @Lazy is required to avoid circular dependency during injection
    private final HydrationEngineService selfProxy;

    private final List<HydrationService<? extends RecordDto>> hydrationPipeline;
    private final HydrationJobService hydrationJobService;
    private final HydrationStepService hydrationStepService;
    private final Executor asyncTaskExecutor;
    private final long hydrationThresholdMinutes;

    private final AtomicReference<Thread> workerThreadRef = new AtomicReference<>();

    public HydrationEngineServiceImpl(@Lazy HydrationEngineService selfProxy,
            List<HydrationService<? extends RecordDto>> hydrationPipeline, HydrationJobService hydrationJobService,
            HydrationStepService hydrationStepService,
            @Qualifier("hydrationAsyncTaskExecutor") Executor hydrationAsyncTaskExecutor,
            @Value("${hydration.threshold.minutes:10}") long hydrationThresholdMinutes) {
        this.selfProxy = selfProxy;
        this.hydrationPipeline = hydrationPipeline;
        this.hydrationJobService = hydrationJobService;
        this.hydrationStepService = hydrationStepService;
        this.asyncTaskExecutor = hydrationAsyncTaskExecutor;
        this.hydrationThresholdMinutes = hydrationThresholdMinutes;
    }

    /*
     * Main trigger method called programmatically with specific types.
     */
    @Override
    public String triggerHydration(RecordTypeEnum... recordTypes) {
        log.info("Hydration triggered");

        if (hydrationJobService.existsInitiatedOrProcessingJob()) {
            String jobId = Optional.ofNullable(hydrationJobService.getCurrentJob()).map(HydrationJob::getId)
                    .orElse("unknown");
            log.info("There is still an ongoing hydration process with job: {}", jobId);
            return jobId;
        }

        log.info("Initializing new job");
        hydrationJobService.initNewJob();

        HydrationJob job = Optional.ofNullable(hydrationJobService.getCurrentJob())
                .orElseGet(() -> HydrationJob.builder().id("unknown").build());
        Map<String, String> loggingContext = MDC.getCopyOfContextMap();

        CompletableFuture.runAsync(() -> {
            workerThreadRef.set(Thread.currentThread());

            if (loggingContext != null) {
                MDC.setContextMap(loggingContext);
            }

            try {
                executeJob(job, recordTypes);
            } finally {
                MDC.clear();
            }
        }, asyncTaskExecutor).orTimeout(hydrationThresholdMinutes, TimeUnit.MINUTES).exceptionally(ex -> {
            if (ex instanceof TimeoutException) {
                log.error("Hydration job {} exceeded {} minutes. Cancelling...", job.getId(),
                        hydrationThresholdMinutes);
                hydrationJobService.cancelStuckJobsAndStepsIfAny();
            } else if (ex instanceof InterruptedException) {
                log.error("Hydration job {} thread was interrupted", job.getId());
                hydrationJobService.cancelStuckJobsAndStepsIfAny();
            }

            if (workerThreadRef.get().isAlive()) {
                workerThreadRef.get().interrupt();
            }

            return null;
        });

        return job.getId();
    }

    private void executeJob(HydrationJob job, RecordTypeEnum... recordTypes) {
        log.info("Starting to execute job: {}", job.getId());
        hydrationJobService.updateJobStatus(job, HydrationStatus.PROCESSING);

        List<RecordTypeEnum> filteringTypes = Arrays.asList(recordTypes);
        List<HydrationService<? extends RecordDto>> servicesToRun = ObjectUtils.isEmpty(filteringTypes)
                ? hydrationPipeline
                : hydrationPipeline.stream().filter(s -> filteringTypes.contains(s.getRecordType())).toList();

        for (HydrationService<? extends RecordDto> service : servicesToRun) {
            if (isThreadInterrupted()) {
                return;
            }

            HydrationStep step = hydrationStepService.initNewStep(service.getRecordType(), job.getId());

            try {
                selfProxy.fetchAndHydrate(service, step);
                hydrationStepService.updateStepStatus(step, HydrationStatus.COMPLETED);
            } catch (Exception e) {
                if (isThreadInterrupted()) {
                    return;
                }

                log.error("Error occurred while processing job: {}", job.getId(), e);
                hydrationStepService.updateStepStatus(step, HydrationStatus.FAILED);
                hydrationJobService.updateJobStatus(job, HydrationStatus.FAILED);
                // Abort processing remaining services in the pipeline due to failure
                return;
            }
        }

        log.info("Hydration completed");
        hydrationJobService.updateJobStatus(job, HydrationStatus.COMPLETED);
    }

    /*
     * Must be public for @Transactional to work. Since it's called from within the
     * same class, we invoke it via selfProxy to ensure Spring applies the
     * transaction proxy.
     *
     * The hydrateDomainData method runs in this @Transactional context, but
     * service.saveHydrationFailedRecord uses @Transactional(propagation =
     * Propagation.REQUIRES_NEW) to create a separate transaction for failure logs,
     * ensuring they are persisted even if the main transaction rolls back.
     */
    @Override
    @Transactional
    public void fetchAndHydrate(HydrationService<? extends RecordDto> service, HydrationStep step) {
        if (isThreadInterrupted()) {
            return;
        }

        Pair<String, Iterable<GetQueryResultsResponse>> responses = service
                .fetchDataFromAthena(service.getDeltaQuery());

        step.setExecutionId(responses.getLeft());
        hydrationStepService.updateStepStatus(step, HydrationStatus.PROCESSING);

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
            if (isThreadInterrupted()) {
                return;
            }

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

    private boolean isThreadInterrupted() {
        Thread t = workerThreadRef.get();
        return t != null && t.isInterrupted();
    }
}
