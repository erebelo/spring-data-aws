package com.erebelo.springdataaws.hydration.repository;

import com.erebelo.springdataaws.hydration.domain.model.HydrationFailedRecord;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface HydrationFailedRecordRepository extends MongoRepository<HydrationFailedRecord, String> {

}
