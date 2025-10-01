package com.erebelo.springdataaws.hydration.repository;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import java.util.Collection;
import java.util.Optional;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface HydrationJobRepository extends MongoRepository<HydrationJob, String> {

    boolean existsByStatusIn(Collection<HydrationStatus> statuses);

    Optional<HydrationJob> findTopByStatusInOrderByRunNumberDesc(Collection<HydrationStatus> statuses);

}
