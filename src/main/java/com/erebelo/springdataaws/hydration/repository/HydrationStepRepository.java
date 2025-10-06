package com.erebelo.springdataaws.hydration.repository;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;
import java.util.Collection;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface HydrationStepRepository extends MongoRepository<HydrationStep, String> {

    List<HydrationStep> findAllByJobIdAndStatusIn(String jobId, Collection<HydrationStatus> statuses);

}
