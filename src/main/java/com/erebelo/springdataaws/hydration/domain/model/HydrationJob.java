package com.erebelo.springdataaws.hydration.domain.model;

import com.erebelo.springdataaws.hydration.domain.enumeration.HydrationStatus;
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
@Document("hydration_jobs")
public class HydrationJob {

    @Id
    private String id;

    private Long runNumber;
    private Instant createdAt;
    private HydrationStatus status;
    private Instant startTime;
    private Instant endTime;

}
