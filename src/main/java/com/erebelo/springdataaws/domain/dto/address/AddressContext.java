package com.erebelo.springdataaws.domain.dto.address;

import java.io.ByteArrayOutputStream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddressContext {

    private String executionId;
    private int processedRecords;
    private ByteArrayOutputStream byteArrayOutputStream; // In-memory output stream

}
