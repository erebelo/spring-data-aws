package com.erebelo.springdataaws.hydration.domain.enumeration;

import com.erebelo.springdataaws.hydration.domain.dto.ContractAdvisorDto;
import com.erebelo.springdataaws.hydration.domain.dto.ContractFirmDto;
import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum RecordTypeEnum {

    CONTRACT_FIRM("CONTRACT_FIRM", ContractFirmDto.class), CONTRACT_ADVISOR("CONTRACT_ADVISOR",
            ContractAdvisorDto.class);

    private final String value;
    private final Class<? extends RecordDto> targetType;

    private static final Map<String, RecordTypeEnum> ENUM_MAP;

    static {
        Map<String, RecordTypeEnum> map = new HashMap<>();
        for (RecordTypeEnum instance : RecordTypeEnum.values()) {
            map.put(instance.getValue(), instance);
        }
        ENUM_MAP = Collections.unmodifiableMap(map);
    }

    public static RecordTypeEnum getRecordTypeEnum(Class<?> targetType) {
        if (targetType == ContractFirmDto.class) {
            return CONTRACT_FIRM;
        } else if (targetType == ContractAdvisorDto.class) {
            return CONTRACT_ADVISOR;
        }

        return null;
    }
}
