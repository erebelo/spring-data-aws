package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.hydration.domain.dto.HydrationJobDto;
import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.service.HydrationJobService;
import com.erebelo.springdataaws.hydration.service.HydrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class HydrationServiceImpl implements HydrationService {

    private final List<DeltaDomainHydrationService<RecordDto>> deltaCommands;
    private final HydrationJobService hydrationJobService;
    private final Executor asyncTaskExecutor;

    @Scheduled(fixedRateString = "${delta.hydration.scheduler.fixed-rate}")
    public String triggerHydration(RecordTypeEnum... recordTypes) {
        if (!hydrationJobService.existsProcessingJob()) {
            HydrationJobDto newJob = this.hydrationJobService.initNewJob();
            Map<String, String> loggingContext = MDC.getCopyOfContextMap(); // Capture the current logging context
            
            CompletableFuture.runAsync(() -> {
                // Restore the logging context in the asynchronous task
                if (loggingContext != null) {
                    MDC.setContextMap(loggingContext);
                }
                try {
                    processJob(newJob, recordTypes);
                } finally {
                    // Clear the logging context after the task completes
                    MDC.clear();
                }
            }, asyncTaskExecutor);
            
            return newJob.getId();
        }

        return hydrationJobService.getCurrentJob().getId();
    }

    protected void processJob(HydrationJobDto newJob, RecordTypeEnum... recordTypes) {
        List<RecordTypeEnum> filteringTypes = List.of(recordTypes);
        List<DeltaDomainHydrationService<RecordDto>> servicesToRun =
                ObjectUtils.isEmpty(filteringTypes) ? deltaCommands :
                        deltaCommands.stream().filter(s -> filteringTypes.contains(s.getRecordTypeEnum())).toList();

        for (DeltaDomainHydrationService<RecordDto> commandService : servicesToRun) {
            DeltaStep step = this.hydrationJobService.initNewStep(commandService.getRecordTypeEnum(), newJob);
            try {
                this.fetchAndHydrate(commandService, step);
                this.hydrationJobService.updateCurrentStepStatus(step, COMPLETED);
            } catch (Exception e) {
                log.error("Error occurred while processing job {}", newJob.getId(), e);
                this.hydrationJobService.updateCurrentStepStatus(step, FAILED);
                this.hydrationJobService.updateCurrentJobStatus(newJob, FAILED);
                // TODO ... undo?
                return;
            }
        }
        this.hydrationJobService.updateCurrentJobStatus(newJob, COMPLETED);
    }

    protected void fetchAndHydrate(DeltaDomainHydrationService<RecordDto> commandService, DeltaStep deltaStep) {
        Pair<String, Iterable<GetQueryResultsResponse>> responses = commandService
                .fetchDataFromAthena(commandService.getDeltaQuery());
        deltaStep.setExecutionId(responses.getLeft());
        this.hydrationJobService.updateCurrentStepStatus(deltaStep, PROCESSING);

        AtomicBoolean headerSkipped = new AtomicBoolean(false);
        Iterator<GetQueryResultsResponse> iterator = responses.getRight().iterator();
        iterator.forEachRemaining(i -> {
            List<Row> rows = i.resultSet().rows();
            rows = skipHeaderOnce(rows, headerSkipped);

            if (!rows.isEmpty()) {
                hydrateDomainData(commandService, rows, deltaStep);
            }
        });
        commandService.doAfterStep();
    }

    protected void hydrateDomainData(DeltaDomainHydrationService<RecordDto> commandService, List<Row> rows,
            DeltaStep deltaStep) {
        List<RecordDto> records = commandService.mapToDomainRecords(rows);
        records.forEach(r -> {
            try {
                RecordDto hydratedRecord = commandService.hydrateDomainData(r).body();
                hydratedRecord.setRecordId(r.getRecordId());
                hydratedRecord.setSubjectId(r.getSubjectId());
                commandService.storeDomainHydrationFinalStatus(hydratedRecord, deltaStep.getExecutionId());
            } catch (Exception e) {
                commandService.storeFailureHydrationStatus(r, e.getMessage(), deltaStep.getExecutionId());
                throw new RuntimeException(String.format(
                        "An error occurred while processing and hydrating %s record. RecordId: %s Error: %s",
                        r.getClass().getSimpleName(), r.getRecordId(), e.getMessage()));
            }
        });
    }

    private List<Row> skipHeaderOnce(List<Row> rows, AtomicBoolean headerSkipped) {
        if (!headerSkipped.getAndSet(true) && !rows.isEmpty()) {
            return rows.subList(1, rows.size());
        }
        return rows;
    }
}

