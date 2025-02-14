package com.erebelo.springdataaws.controller;

import static com.erebelo.springdataaws.constant.BusinessConstant.ADDRESSES_FEED_PATH;
import static com.erebelo.springdataaws.constant.BusinessConstant.ADDRESSES_PATH;
import static com.erebelo.springdataaws.mock.AddressMock.EXECUTION_ID;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.erebelo.springdataaws.exception.model.BadRequestException;
import com.erebelo.springdataaws.service.AddressService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = AddressController.class, excludeAutoConfiguration = SecurityAutoConfiguration.class)
class AddressControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AddressService service;

    @Test
    void testAddressFeedTriggerSuccessful() throws Exception {
        given(service.addressFeedTrigger()).willReturn(EXECUTION_ID);

        mockMvc.perform(post(ADDRESSES_PATH + ADDRESSES_FEED_PATH).accept(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value(200)).andExpect(jsonPath("$.body")
                        .value("Address feed execution created successfully. Execution ID: '" + EXECUTION_ID + "'"));

        verify(service).addressFeedTrigger();
    }

    @Test
    void testAddressFeedTriggerFailure() throws Exception {
        String errorMsg = "Error: 'Failed to trigger address feed'. Execution ID: "
                + "'3e0135ac-d582-4cb2-b671-f74c945d13e2'. Root Cause: 'Unable to load credentials'.";
        given(service.addressFeedTrigger()).willThrow(new BadRequestException(errorMsg));

        mockMvc.perform(post(ADDRESSES_PATH + ADDRESSES_FEED_PATH).accept(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.status").value("BAD_REQUEST"))
                .andExpect(jsonPath("$.message").value(errorMsg)).andExpect(jsonPath("$.timestamp").isNotEmpty());

        verify(service).addressFeedTrigger();
    }
}
