package com.erebelo.springdataaws.dto.address;

import java.io.ByteArrayOutputStream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddressContextDto {

    private String executionId;
    private int processedRecords;
    private ByteArrayOutputStream byteArrayOutputStream; // In-memory output stream

}
