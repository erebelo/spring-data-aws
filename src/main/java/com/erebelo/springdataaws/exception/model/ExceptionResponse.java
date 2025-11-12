package com.erebelo.springdataaws.exception.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.http.HttpStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ExceptionResponse(HttpStatus status, String message, Long timestamp) {
}
