package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.domain.dto.AthenaQueryDto;
import com.erebelo.springdataaws.hydration.domain.dto.HydrationRunDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import com.erebelo.springdataaws.hydration.repository.HydrationJobRepository;
import com.erebelo.springdataaws.hydration.repository.contract.HydrationRunQueries;
import com.erebelo.springdataaws.hydration.service.HydrationJobService;
import com.erebelo.springdataaws.hydration.service.HydrationStepService;
import com.erebelo.springdataaws.service.AthenaService;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;

@Service
public class HydrationJobServiceImpl implements HydrationJobService {

    private final AthenaService athenaService;
    private final HydrationStepService hydrationStepService;
    private final HydrationJobRepository repository;
    private final HydrationRunQueries hydrationRunQueries;

    public HydrationJobServiceImpl(@Qualifier("hydrationAthenaService") AthenaService athenaService,
            HydrationStepService hydrationStepService, HydrationJobRepository repository,
            HydrationRunQueries hydrationRunQueries) {
        this.athenaService = athenaService;
        this.hydrationStepService = hydrationStepService;
        this.repository = repository;
        this.hydrationRunQueries = hydrationRunQueries;
    }

    @Getter
    private HydrationJob currentJob;

    @Override
    public boolean existsInitiatedOrProcessingJob() {
        return repository.existsByStatusIn(List.of(HydrationStatus.INITIATED, HydrationStatus.PROCESSING));
    }

    @Override
    public void cancelStuckJobsAndStepsIfAny() {
        Optional<HydrationJob> lastActiveJob = repository
                .findTopByStatusInOrderByRunNumberDesc(List.of(HydrationStatus.INITIATED, HydrationStatus.PROCESSING));

        if (lastActiveJob.isEmpty()) {
            this.currentJob = null;
            return;
        }

        Instant now = Instant.now();
        HydrationJob job = lastActiveJob.get();
        job.setStatus(HydrationStatus.CANCELED);
        job.setEndTime(now);

        hydrationStepService.cancelActiveStepsByJobId(job.getId(), now);
        repository.save(job);
        this.currentJob = null;
    }

    @Override
    public HydrationJob initNewJob() {
        Optional<HydrationJob> lastJobRun = repository
                .findTopByStatusInOrderByRunNumberDesc(List.of(HydrationStatus.COMPLETED, HydrationStatus.FAILED));

        Long nextRunNumber = lastJobRun.map(job -> job.getRunNumber() + 1).orElse(1L);
        HydrationRunDto hydrationRun = fetchHydrationRunFromAthena(nextRunNumber);

        if (hydrationRun == null) {
            throw new IllegalStateException("No hydration run found for runNumber: " + nextRunNumber);
        }

        HydrationJob newJob = HydrationJob.builder().runNumber(nextRunNumber).createdAt(hydrationRun.getCreatedAt())
                .startTime(Instant.now()).status(HydrationStatus.INITIATED).build();

        repository.save(newJob);
        this.currentJob = newJob;

        return newJob;
    }

    @Override
    public void updateJobStatus(HydrationJob job, HydrationStatus status) {
        job.setStatus(status);
        this.currentJob = job;

        if (HydrationStatus.FAILED == status || HydrationStatus.COMPLETED == status) {
            job.setEndTime(Instant.now());
            this.currentJob = null;
        }

        repository.save(job);
    }

    private HydrationRunDto fetchHydrationRunFromAthena(Long runNumber) {
        AthenaQueryDto athenaQuery = athenaService
                .submitAthenaQuery(hydrationRunQueries.getHydrationRunsDataQuery(runNumber));
        String executionId = athenaQuery.getExecutionId();

        try {
            athenaService.waitForQueryToComplete(executionId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted while waiting for Athena query to complete", e);
        }

        Iterator<GetQueryResultsResponse> iterator = athenaService.getQueryResults(executionId).iterator();

        if (iterator.hasNext()) {
            List<Row> rows = iterator.next().resultSet().rows();

            if (rows.size() > 1) {
                String[] athenaColumnOrder = rows.getFirst().data().stream().map(Datum::varCharValue)
                        .toArray(String[]::new);
                List<HydrationRunDto> hydrationRuns = athenaService.mapRowsToClass(athenaColumnOrder,
                        rows.subList(1, rows.size()), HydrationRunDto.class);

                return hydrationRuns.getFirst();
            }
        }

        return null;
    }
}
