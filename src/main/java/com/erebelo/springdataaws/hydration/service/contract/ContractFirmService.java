package com.erebelo.springdataaws.hydration.service.contract;

import com.erebelo.springdataaws.hydration.domain.dto.ContractFirmDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.repository.contract.ContractQueries;
import com.erebelo.springdataaws.hydration.service.AbstractHydrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ContractFirmService extends AbstractHydrationService<ContractFirmDto> {

    private final ContractQueries contractQueries;

    protected ContractFirmService(ContractQueries contractQueries) {
        super(ContractFirmDto.class);
        this.contractQueries = contractQueries;
    }

    @Override
    public RecordTypeEnum getRecordType() {
        return RecordTypeEnum.CONTRACT_FIRM;
    }

    @Override
    public String getDeltaQuery() {
        return contractQueries.getFirmContractDataQuery();
    }

    @Override
    public ContractFirmDto hydrateDomainData(ContractFirmDto domainData) {
        log.info("Hydrating Contract Firm");
        return domainData;
    }
}
