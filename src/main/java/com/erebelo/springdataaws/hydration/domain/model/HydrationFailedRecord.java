package com.erebelo.springdataaws.hydration.domain.model;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
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
@Document("hydration_failed_records")
public class HydrationFailedRecord extends RecordDto {

    @Id
    private String id;

    private String stepId;
    private String executionId;
    private RecordTypeEnum domainType;
    private String errorMessage;

}
