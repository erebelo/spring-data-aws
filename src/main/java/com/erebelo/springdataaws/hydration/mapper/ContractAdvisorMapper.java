package com.erebelo.springdataaws.hydration.mapper;

import static org.mapstruct.ReportingPolicy.WARN;

import com.erebelo.springdataaws.hydration.domain.dto.ContractAdvisorDto;
import com.erebelo.springdataaws.hydration.domain.model.ContractAdvisor;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring", unmappedTargetPolicy = WARN)
public interface ContractAdvisorMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "legacyId", source = "id")
    ContractAdvisor dtoToEntity(ContractAdvisorDto dto);

}
