package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;

public interface HydrationEngineService {

    String triggerHydration(RecordTypeEnum... recordTypes);

    HydrationJob initJobIfNoneRunning();

    void executeJob(HydrationJob job, RecordTypeEnum... recordTypes);

}
