package com.erebelo.springdataaws.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.erebelo.springdataaws.exception.model.AthenaQueryException;
import com.erebelo.springdataaws.exception.model.ExceptionResponse;
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
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.status());
        assertEquals("Test exception message", response.message());
    }

    @Test
    void testHandleExceptionWithEmptyMessage() {
        Exception exception = new Exception("");

        ExceptionResponse response = globalExceptionHandler.handleException(exception);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.status());
        assertEquals("No defined message", response.message());
    }

    @Test
    void testHandleAthenaQueryException() {
        AthenaQueryException exception = new AthenaQueryException("Athena query failed");

        ExceptionResponse response = globalExceptionHandler.handleAthenaQueryException(exception);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.status());
        assertEquals("Athena query failed", response.message());
    }
}
