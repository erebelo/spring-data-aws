package com.erebelo.springdataaws.hydration.repository.contract;

import com.erebelo.springdataaws.hydration.domain.model.ContractFirm;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContractFirmRepository extends MongoRepository<ContractFirm, String> {
}
