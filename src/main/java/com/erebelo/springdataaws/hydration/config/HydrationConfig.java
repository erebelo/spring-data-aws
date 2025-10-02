package com.erebelo.springdataaws.hydration.config;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.service.HydrationService;
import com.erebelo.springdataaws.hydration.service.contract.ContractAdvisorService;
import com.erebelo.springdataaws.hydration.service.contract.ContractFirmService;
import java.util.List;
import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableScheduling
public class HydrationConfig {

    @Bean
    @SuppressWarnings("squid:S1452")
    public List<HydrationService<? extends RecordDto>> hydrationPipeline(ContractAdvisorService contractAdvisorService,
            ContractFirmService contractFirmService) {
        return List.of(contractAdvisorService, contractFirmService);
    }

    /*
     * Dedicated executor for manual asynchronous hydration triggered via
     * CompletableFuture. By declaring it as a separate bean, we ensure that manual
     * triggers execute independently of scheduled tasks and do not compete with
     * them for threads.
     */
    @Bean
    public Executor asyncTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("Hyd-Executor-");
        executor.initialize();
        return executor;
    }

    /*
     * Executor dedicated for scheduled tasks (@Scheduled methods). By having a
     * dedicated ThreadPoolTaskScheduler, scheduled jobs run on their own threads
     * and are isolated from manual async executions, avoiding conflicts or thread
     * starvation.
     */
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("Hyd-Scheduler-");
        scheduler.initialize();
        return scheduler;
    }
}
