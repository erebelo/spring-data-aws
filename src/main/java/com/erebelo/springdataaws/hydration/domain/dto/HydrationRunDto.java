package com.erebelo.springdataaws.hydration.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HydrationRunDto {

    @JsonProperty("run_number")
    private Long runNumber;

    @JsonProperty("created_at")
    private Instant createdAt;

}
