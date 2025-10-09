package com.erebelo.springdataaws.hydration.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ContractFirmDto extends RecordDto {

    private String id;

    private String name;

    @JsonProperty("registration_number")
    private String registrationNumber;

    @JsonProperty("tax_id")
    private String taxId;

    @JsonProperty("start_date")
    private LocalDate startDate;

    @JsonProperty("end_date")
    private LocalDate endDate;

}
