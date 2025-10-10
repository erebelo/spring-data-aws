package com.erebelo.springdataaws.hydration.service.contract;

import com.erebelo.springdataaws.hydration.domain.dto.ContractFirmDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.model.HydrationJob;
import com.erebelo.springdataaws.hydration.mapper.ContractFirmMapper;
import com.erebelo.springdataaws.hydration.query.ContractQueries;
import com.erebelo.springdataaws.hydration.repository.contract.ContractFirmRepository;
import com.erebelo.springdataaws.hydration.service.AbstractHydrationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ContractFirmService extends AbstractHydrationService<ContractFirmDto> {

    private final ContractQueries contractQueries;
    private final ContractFirmMapper mapper;
    private final ContractFirmRepository repository;

    protected ContractFirmService(ContractQueries contractQueries, ContractFirmMapper mapper,
            ContractFirmRepository repository) {
        super(ContractFirmDto.class);
        this.contractQueries = contractQueries;
        this.mapper = mapper;
        this.repository = repository;
    }

    @Override
    public RecordTypeEnum getRecordType() {
        return RecordTypeEnum.CONTRACT_FIRM;
    }

    @Override
    public String getDeltaQuery() {
        HydrationJob currentJob = this.hydrationJobService.getCurrentJob();
        return contractQueries.getFirmContractsDataQuery(currentJob.getRunNumber());
    }

    @Override
    public ContractFirmDto hydrateDomainData(ContractFirmDto domainData) {
        log.info("Hydrating Contract Advisor with recordId: {}", domainData.getRecordId());
        repository.save(mapper.dtoToEntity(domainData));
        return domainData;
    }
}
