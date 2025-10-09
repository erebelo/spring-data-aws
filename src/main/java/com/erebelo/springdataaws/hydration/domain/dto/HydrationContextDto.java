package com.erebelo.springdataaws.hydration.domain.dto;

import com.erebelo.springdataaws.domain.dto.AthenaContextDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class HydrationContextDto extends AthenaContextDto {
}
