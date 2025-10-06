package com.erebelo.springdataaws.hydration.controller;

import static com.erebelo.springdataaws.hydration.constant.BusinessConstant.HYDRATION_PATH;
import static com.erebelo.springdataaws.hydration.constant.BusinessConstant.START_HYDRATION_PATH;

import com.erebelo.springdataaws.domain.response.BaseResponse;
import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import com.erebelo.springdataaws.hydration.domain.request.HydrationRequest;
import com.erebelo.springdataaws.hydration.service.HydrationEngineService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(HYDRATION_PATH)
@RequiredArgsConstructor
public class HydrationController {

    private final HydrationEngineService service;

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PostMapping(value = START_HYDRATION_PATH, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public BaseResponse triggerHydration(@RequestBody(required = false) HydrationRequest request) {
        List<RecordTypeEnum> recordTypes = request != null ? request.recordTypes() : List.of();
        String recordTypesLog = recordTypes.stream().map(RecordTypeEnum::getValue).toList().toString();
        log.info("POST {} with recordTypes={}", HYDRATION_PATH + START_HYDRATION_PATH, recordTypesLog);

        RecordTypeEnum[] recordTypeArray = recordTypes.toArray(new RecordTypeEnum[0]);
        return new BaseResponse(HttpStatus.ACCEPTED.value(),
                "Hydration started with Job Id: '" + service.triggerHydration(recordTypeArray) + "'");
    }
}
