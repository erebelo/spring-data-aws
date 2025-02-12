package com.erebelo.springdataaws.controller;

import static com.erebelo.springdataaws.constant.BusinessConstant.ADDRESSES_PATH;
import static com.erebelo.springdataaws.constant.BusinessConstant.ADDRESSES_TRIGGER_PATH;

import com.erebelo.springdataaws.domain.dto.athena.AthenaQueryDto;
import com.erebelo.springdataaws.service.AddressService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(ADDRESSES_PATH)
@RequiredArgsConstructor
public class AddressController {

    private final AddressService service;

    @PostMapping(value = ADDRESSES_TRIGGER_PATH, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AthenaQueryDto> addressTrigger() {
        log.info("POST {}", ADDRESSES_PATH + ADDRESSES_TRIGGER_PATH);
        return ResponseEntity.ok(service.addressTrigger());
    }
}
