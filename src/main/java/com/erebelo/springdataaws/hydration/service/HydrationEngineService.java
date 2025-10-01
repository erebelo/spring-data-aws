package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;

public interface HydrationEngineService {

    String triggerHydration(RecordTypeEnum... recordTypes);

}
