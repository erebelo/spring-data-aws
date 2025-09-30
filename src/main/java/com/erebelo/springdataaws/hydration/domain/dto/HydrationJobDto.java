package com.erebelo.springdataaws.hydration.domain.dto;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("hydration_jobs")
public class HydrationJobDto {

    @Id
    private String id;

    private Long jobId;
    private Instant processStartTime;
    private Instant processEndTime;
    private HydrationStatus status;

}
