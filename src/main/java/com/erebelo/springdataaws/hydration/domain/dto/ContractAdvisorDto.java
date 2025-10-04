package com.erebelo.springdataaws.hydration.domain.dto;

import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ContractAdvisorDto extends RecordDto {

    private String id;
    private String firstName;
    private String lastName;
    private String licenseNumber;
    private LocalDate startDate;
    private LocalDate endDate;

}
