package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;

class AthenaClientConfigurationTest {

    @Test
    void testAthenaClientSuccessful() {
        AthenaClientConfiguration provider = new AthenaClientConfiguration();
        AthenaClient client = provider.athenaClient();
        assertNotNull(client, "The AthenaClient should not be null");
    }

    @Test
    void testAthenaClientFailure() {
        AthenaClientConfiguration provider = new AthenaClientConfiguration() {
            @Override
            public AthenaClient athenaClient() {
                throw new RuntimeException("Failed to create AthenaClient");
            }
        };

        Exception exception = assertThrows(RuntimeException.class, provider::athenaClient);
        assertEquals("Failed to create AthenaClient", exception.getMessage());
    }
}
