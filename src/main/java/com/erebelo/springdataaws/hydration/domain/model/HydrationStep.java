package com.erebelo.springdataaws.hydration.domain.model;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("hydration_steps")
public class HydrationStep {

    @Id
    private String id;

    private String jobId;
    private String executionId;
    private RecordTypeEnum domainType;
    private HydrationStatus status;
    private Instant startTime;
    private Instant endTime;

}
