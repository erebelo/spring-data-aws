package com.erebelo.springdataaws.hydration.service.impl;

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
@ConditionalOnProperty(name = "hydration.scheduled.enabled", havingValue = "true")
public class HydrationEngineScheduledService {

    private final HydrationEngineService hydrationEngineService;

    /*
     * Ensures that only one hydration job runs at a time. The scheduled method is
     * triggered according to the cron expression. If a previous job is still
     * running when the next trigger occurs, that execution is skipped, preventing
     * concurrent processing.
     */
    @Scheduled(cron = "${hydration.scheduled.cron:0 */15 * * * *}")
    public void scheduledTrigger() {
        log.info("Hydration triggered by scheduler");
        hydrationEngineService.triggerHydration();
    }
}
