package com.erebelo.springdataaws.hydration.service.contract;

import com.erebelo.springdataaws.hydration.domain.dto.ContractAdvisorDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.repository.contract.ContractQueries;
import com.erebelo.springdataaws.hydration.service.AbstractHydrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ContractAdvisorService extends AbstractHydrationService<ContractAdvisorDto> {

    private final ContractQueries contractQueries;

    protected ContractAdvisorService(ContractQueries contractQueries) {
        super(ContractAdvisorDto.class);
        this.contractQueries = contractQueries;
    }

    @Override
    public RecordTypeEnum getRecordType() {
        return RecordTypeEnum.CONTRACT_ADVISOR;
    }

    @Override
    public String getDeltaQuery() {
        return contractQueries.getAdvisorContractDataQuery();
    }

    @Override
    public ContractAdvisorDto hydrateDomainData(ContractAdvisorDto domainData) {
        log.info("Hydrating Contract Advisor");
        return domainData;
    }
}
