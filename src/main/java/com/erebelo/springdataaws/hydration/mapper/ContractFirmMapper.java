package com.erebelo.springdataaws.hydration.mapper;

import static org.mapstruct.ReportingPolicy.WARN;

import com.erebelo.springdataaws.hydration.domain.dto.ContractFirmDto;
import com.erebelo.springdataaws.hydration.domain.model.ContractFirm;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring", unmappedTargetPolicy = WARN)
public interface ContractFirmMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "legacyId", source = "id")
    @Mapping(target = "recordId", ignore = true)
    ContractFirm dtoToEntity(ContractFirmDto dto);

}
