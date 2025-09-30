package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.dto.HydrationJobDto;

public interface HydrationJobService {

    boolean existsProcessingJob();

    HydrationJobDto initNewJob();

    HydrationJobDto getCurrentJob();

}
