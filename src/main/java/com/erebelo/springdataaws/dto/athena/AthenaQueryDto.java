package com.erebelo.springdataaws.dto.athena;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AthenaQueryDto {

    private String executionId;

}
