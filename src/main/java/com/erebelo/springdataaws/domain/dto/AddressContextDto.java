package com.erebelo.springdataaws.domain.dto;

import java.io.ByteArrayOutputStream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AddressContextDto extends AthenaContextDto {

    private String executionId;
    private boolean headerWritten;
    private int processedRecords;
    private ByteArrayOutputStream byteArrayOutputStream; // In-memory output stream

}
