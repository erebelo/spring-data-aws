package com.erebelo.springdataaws.controller;

import static com.erebelo.springdataaws.constant.BusinessConstant.ADDRESSES_FEED_TRIGGER_PATH;
import static com.erebelo.springdataaws.constant.BusinessConstant.ADDRESSES_PATH;

import com.erebelo.springdataaws.domain.response.AddressResponse;
import com.erebelo.springdataaws.service.AddressService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(ADDRESSES_PATH)
@RequiredArgsConstructor
public class AddressController {

    private final AddressService service;

    @ResponseStatus(HttpStatus.OK)
    @PostMapping(value = ADDRESSES_FEED_TRIGGER_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
    public AddressResponse addressFeedTrigger() {
        log.info("POST {}", ADDRESSES_PATH + ADDRESSES_FEED_TRIGGER_PATH);
        return new AddressResponse(HttpStatus.OK.value(), "Address feed execution created successfully. "
                + "Execution ID: '" + service.addressFeedTrigger() + "'");
    }
}
