package com.erebelo.springdataaws.domain.dto;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AddressDto extends RecordDto {

    @JsonProperty("address_id")
    private String addressId;

    @JsonProperty("home_address_line_1")
    private String homeAddressLine1;

    @JsonProperty("home_address_line_2")
    private String homeAddressLine2;

    @JsonProperty("home_city")
    private String homeCity;

    @JsonProperty("home_state")
    private String homeState;

    @JsonProperty("home_zip_code")
    private String homeZipCode;

    @JsonProperty("home_country")
    private String homeCountry;

    @JsonProperty("work_address_line_1")
    private String workAddressLine1;

    @JsonProperty("work_address_line_2")
    private String workAddressLine2;

    @JsonProperty("work_city")
    private String workCity;

    @JsonProperty("work_state")
    private String workState;

    @JsonProperty("work_zip_code")
    private String workZipCode;

    @JsonProperty("work_country")
    private String workCountry;

    @JsonProperty("other_address_line_1")
    private String otherAddressLine1;

    @JsonProperty("other_address_line_2")
    private String otherAddressLine2;

    @JsonProperty("other_city")
    private String otherCity;

    @JsonProperty("other_state")
    private String otherState;

    @JsonProperty("other_zip_code")
    private String otherZipCode;

    @JsonProperty("other_country")
    private String otherCountry;

}
