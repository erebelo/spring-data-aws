package com.erebelo.springdataaws.exception.model;

public class BadRequestException extends RuntimeException {

    public BadRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadRequestException(String message) {
        super(message);
    }
}
