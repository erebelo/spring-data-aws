package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import com.erebelo.springdataaws.hydration.repository.HydrationJobRepository;
import com.erebelo.springdataaws.hydration.service.HydrationJobService;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HydrationJobServiceImpl implements HydrationJobService {

    private final HydrationJobRepository repository;

    @Getter
    private HydrationJob currentJob;

    @Override
    public boolean existsInitiatedOrProcessingJob() {
        return repository.existsByStatusIn(List.of(HydrationStatus.INITIATED, HydrationStatus.PROCESSING));
    }

    @Override
    public HydrationJob initNewJob() {
        Optional<HydrationJob> lastJobRun = repository
                .findTopByStatusInOrderByRunNumberDesc(List.of(HydrationStatus.COMPLETED, HydrationStatus.FAILED));

        long nextRunNumber = lastJobRun.map(job -> job.getRunNumber() + 1).orElse(1L);
        HydrationJob newJob = HydrationJob.builder().runNumber(nextRunNumber).startTime(Instant.now())
                .status(HydrationStatus.INITIATED).build();

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
}
