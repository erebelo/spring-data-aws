package com.erebelo.springdataaws.hydration.domain.model;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Document("firm_contracts")
public class ContractFirm extends RecordDto {

    @Id
    private String id;

    private String legacyId;
    private String name;
    private String registrationNumber;
    private String taxId;
    private LocalDate startDate;
    private LocalDate endDate;

}
