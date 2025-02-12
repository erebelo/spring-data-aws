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
public class AddressDto {

    @JsonProperty("addressid")
    private String addressId;

    @JsonProperty("home_city")
    private String homeCity;

    @JsonProperty("home_state")
    private String homeState;

    @JsonProperty("home_country")
    private String homeCountry;

    @JsonProperty("work_city")
    private String workCity;

    @JsonProperty("work_state")
    private String workState;

    @JsonProperty("work_country")
    private String workCountry;

    @JsonProperty("other_city")
    private String otherCity;

    @JsonProperty("other_state")
    private String otherState;

    @JsonProperty("other_country")
    private String otherCountry;

}
