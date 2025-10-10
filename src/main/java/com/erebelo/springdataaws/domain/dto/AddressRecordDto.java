package com.erebelo.springdataaws.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class AddressRecordDto {

    // recordId can be read (deserialized) from input but not written to output
    // (serializing)
    @JsonProperty(value = "record_id", access = JsonProperty.Access.WRITE_ONLY)
    private String recordId;

}
