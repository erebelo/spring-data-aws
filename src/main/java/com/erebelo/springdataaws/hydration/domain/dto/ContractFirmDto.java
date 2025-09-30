package com.erebelo.springdataaws.hydration.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.LocalDate;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ContractFirmDto extends RecordDto {

    private String name;
    private String registrationNumber;
    private String taxId;
    private LocalDate startDate;
    private LocalDate endDate;

}
