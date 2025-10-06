package com.erebelo.springdataaws.hydration.service;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;

public interface HydrationStepService {

    void cancelActiveStepsByJobId(String jobId);

    HydrationStep initNewStep(RecordTypeEnum recordType, String jobId);

    void updateStepStatus(HydrationStep step, HydrationStatus status);

}
