package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;

public interface HydrationEngineService {

    String triggerHydration(RecordTypeEnum... recordTypes);

    HydrationJob initJobIfNoneRunning();

    void executeJob(HydrationJob job, RecordTypeEnum... recordTypes);

    void fetchAndHydrate(HydrationService<? extends RecordDto> service, HydrationJob job, HydrationStep step);

}
