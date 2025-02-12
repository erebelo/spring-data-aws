package com.erebelo.springdataaws.exception;

public class AthenaQueryException extends RuntimeException {

    public AthenaQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public AthenaQueryException(String message) {
        super(message);
    }
}
