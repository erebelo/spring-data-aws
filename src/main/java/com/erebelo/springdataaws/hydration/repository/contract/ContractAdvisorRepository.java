package com.erebelo.springdataaws.hydration.repository.contract;

import com.erebelo.springdataaws.hydration.domain.model.ContractAdvisor;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContractAdvisorRepository extends MongoRepository<ContractAdvisor, String> {
}
