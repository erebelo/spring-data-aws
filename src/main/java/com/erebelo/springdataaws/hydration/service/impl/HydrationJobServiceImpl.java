package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.hydration.domain.dto.HydrationJobDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.repository.HydrationJobRepository;
import com.erebelo.springdataaws.hydration.service.HydrationJobService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class HydrationJobServiceImpl implements HydrationJobService {

    private final HydrationJobRepository repository;

    @Getter
    private HydrationJobDto currentJob;

    @Override
    public boolean existsProcessingJob() {
        return repository.existsByStatus(HydrationStatus.PROCESSING);
    }

    @Override
    public HydrationJobDto initNewJob() {
        Optional<HydrationJobDto> lastJobRun = repository.findTopByStatusOrderByJobIdDesc(HydrationStatus.COMPLETED);

        long nextJobId = lastJobRun.map(job -> job.getJobId() + 1).orElse(1L);
        HydrationJobDto newJob = HydrationJobDto.builder().jobId(nextJobId).processStartTime(Instant.now()).status(HydrationStatus.PROCESSING).build();

        this.currentJob = newJob;
        return newJob;
    }
}
