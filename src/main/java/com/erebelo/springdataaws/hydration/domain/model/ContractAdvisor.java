package com.erebelo.springdataaws.hydration.domain.model;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import java.time.LocalDate;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Document("advisor_contracts")
public class ContractAdvisor extends RecordDto {

    @Id
    private String id;

    private String legacyId;
    private String firstName;
    private String lastName;
    private String licenseNumber;
    private LocalDate startDate;
    private LocalDate endDate;

}
