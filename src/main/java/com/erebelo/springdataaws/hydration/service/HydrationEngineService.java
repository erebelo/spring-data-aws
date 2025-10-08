package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;

public interface HydrationEngineService {

    String triggerHydration(RecordTypeEnum... recordTypes);

    void fetchAndHydrate(HydrationService<? extends RecordDto> service, HydrationStep step);

}
