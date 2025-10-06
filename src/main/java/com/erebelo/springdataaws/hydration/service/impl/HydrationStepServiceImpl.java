package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;
import com.erebelo.springdataaws.hydration.repository.HydrationStepRepository;
import com.erebelo.springdataaws.hydration.service.HydrationStepService;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HydrationStepServiceImpl implements HydrationStepService {

    private final HydrationStepRepository repository;

    @Override
    public HydrationStep initNewStep(RecordTypeEnum recordType, String jobId) {
        HydrationStep newStep = HydrationStep.builder().jobId(jobId).startTime(Instant.now()).domainType(recordType)
                .status(HydrationStatus.INITIATED).build();

        repository.save(newStep);

        return newStep;
    }

    @Override
    public void updateStepStatus(HydrationStep step, HydrationStatus status) {
        step.setStatus(status);

        if (HydrationStatus.FAILED == status || HydrationStatus.COMPLETED == status) {
            step.setEndTime(Instant.now());
        }

        repository.save(step);
    }

    @Override
    public void cancelActiveStepsByJobId(String jobId) {
        List<HydrationStep> activeSteps = repository.findAllByJobIdAndStatusIn(jobId,
                List.of(HydrationStatus.INITIATED, HydrationStatus.PROCESSING));

        if (activeSteps.isEmpty()) {
            return;
        }

        Instant now = Instant.now();
        activeSteps.forEach(step -> {
            step.setStatus(HydrationStatus.CANCELED);
            step.setEndTime(now);
        });

        repository.saveAll(activeSteps);
    }
}
