package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;

public interface HydrationJobService {

    HydrationJob getCurrentJob();

    boolean existsInitiatedOrProcessingJob();

    HydrationJob initNewJob();

    void updateJobStatus(HydrationJob job, HydrationStatus status);

}
