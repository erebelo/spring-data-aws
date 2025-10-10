package com.erebelo.springdataaws.exception;

import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public @ResponseBody ExceptionResponse handleException(Exception exception) {
        log.error("Exception thrown:", exception);
        return createExceptionResponse(HttpStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
    }

    @ExceptionHandler(AthenaQueryException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public @ResponseBody ExceptionResponse handleAthenaQueryException(AthenaQueryException exception) {
        log.error("AthenaQueryException thrown:", exception);
        return createExceptionResponse(HttpStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
    }

    private ExceptionResponse createExceptionResponse(final HttpStatus httpStatus, final String message) {
        String errorMessage = ObjectUtils.isEmpty(message) ? "No defined message" : message;
        return new ExceptionResponse(httpStatus, errorMessage, System.currentTimeMillis());
    }
}
