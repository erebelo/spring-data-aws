package com.erebelo.springdataaws.hydration.repository;

import com.erebelo.springdataaws.hydration.domain.dto.HydrationJobDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface HydrationJobRepository extends MongoRepository<HydrationJobDto, String> {

    boolean existsByStatus(HydrationStatus status);

    Optional<HydrationJobDto> findTopByStatusOrderByJobIdDesc(HydrationStatus status);

}
