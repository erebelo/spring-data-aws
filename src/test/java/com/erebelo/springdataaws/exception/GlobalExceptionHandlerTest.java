package com.erebelo.springdataaws.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class GlobalExceptionHandlerTest {

    @InjectMocks
    private GlobalExceptionHandler globalExceptionHandler;

    @Test
    void testHandleException() {
        Exception exception = new Exception("Test exception message");

        ExceptionResponse response = globalExceptionHandler.handleException(exception);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatus());
        assertEquals("Test exception message", response.getMessage());
    }

    @Test
    void testHandleExceptionWithEmptyMessage() {
        Exception exception = new Exception("");

        ExceptionResponse response = globalExceptionHandler.handleException(exception);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatus());
        assertEquals("No defined message", response.getMessage());
    }

    @Test
    void testHandleAthenaQueryException() {
        AthenaQueryException exception = new AthenaQueryException("Athena query failed");

        ExceptionResponse response = globalExceptionHandler.handleAthenaQueryException(exception);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatus());
        assertEquals("Athena query failed", response.getMessage());
    }
}
