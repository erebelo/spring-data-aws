package com.erebelo.springdataaws.exception.model;

public class AthenaQueryException extends RuntimeException {

    public AthenaQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public AthenaQueryException(String message) {
        super(message);
    }
}
