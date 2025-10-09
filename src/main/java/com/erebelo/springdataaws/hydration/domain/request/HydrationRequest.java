package com.erebelo.springdataaws.hydration.domain.request;

import com.erebelo.springdataaws.hydration.domain.enumeration.RecordTypeEnum;
import java.util.List;

public record HydrationRequest(List<RecordTypeEnum> recordTypes) {
}
