package com.erebelo.springdataaws.dto.address;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddressBundleDto {

    @JsonProperty("recordid")
    private String recordId;

    @JsonProperty("addressid")
    private String addressId;

    @JsonProperty("home_addresstype")
    private String homeAddressType;

    @JsonProperty("home_addressline1")
    private String homeAddressLine1;

    @JsonProperty("home_addressline2")
    private String homeAddressLine2;

    @JsonProperty("home_city")
    private String homeCity;

    @JsonProperty("home_state")
    private String homeState;

    @JsonProperty("home_country")
    private String homeCountry;

    @JsonProperty("home_zipcode")
    private String homeZipCode;

    @JsonProperty("work_addresstype")
    private String workAddressType;

    @JsonProperty("work_addressline1")
    private String workAddressLine1;

    @JsonProperty("work_addressline1")
    private String workAddressLine2;

    @JsonProperty("work_city")
    private String workCity;

    @JsonProperty("work_state")
    private String workState;

    @JsonProperty("work_country")
    private String workCountry;

    @JsonProperty("work_zipcode")
    private String workZipCode;

    @JsonProperty("other_addresstype")
    private String otherAddressType;

    @JsonProperty("other_addressline1")
    private String otherAddressLine1;

    @JsonProperty("other_addressline2")
    private String otherAddressLine2;

    @JsonProperty("other_city")
    private String otherCity;

    @JsonProperty("other_state")
    private String otherState;

    @JsonProperty("other_country")
    private String otherCountry;

    @JsonProperty("other_zipcode")
    private String otherZipCode;

}
