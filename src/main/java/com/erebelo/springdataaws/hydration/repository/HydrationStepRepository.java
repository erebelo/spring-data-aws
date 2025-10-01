package com.erebelo.springdataaws.hydration.repository;

import com.erebelo.springdataaws.hydration.domain.model.HydrationStep;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface HydrationStepRepository extends MongoRepository<HydrationStep, String> {

}
