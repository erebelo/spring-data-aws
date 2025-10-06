package com.erebelo.springdataaws.hydration.service.impl;

import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import com.erebelo.springdataaws.hydration.service.HydrationEngineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/*
 * Scheduled method called automatically by Spring.
 * No-arg as required by @Scheduled.
 * The @ConditionalOnProperty ensures this job runs only when 'hydration.scheduler.enabled=true'.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "hydration.scheduler.enabled", havingValue = "true")
public class HydrationEngineScheduledService {

    private final HydrationEngineService hydrationEngineService;

    @Scheduled(fixedRateString = "${hydration.scheduler.fixed-rate}")
    public void scheduledTrigger() {
        HydrationJob job = hydrationEngineService.initJobIfNoneRunning();

        if (job != null) {
            log.info("Hydration triggered by scheduler");
            hydrationEngineService.executeJob(job);
        }
    }
}
