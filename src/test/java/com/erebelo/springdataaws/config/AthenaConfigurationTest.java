package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;

class AthenaConfigurationTest {

    private static final String ATHENA_DB = "db_test";
    private static final String OUTPUT_BUCKET = "s3://test-output-bucket";
    private static final String ATHENA_HYDRATION_DB = "hydration_db_test";
    private static final String HYDRATION_OUTPUT_BUCKET = "s3://test-hydration-output-bucket";

    @Test
    void testAthenaClientSuccessful() {
        AthenaConfiguration provider = new AthenaConfiguration();
        AthenaClient client = provider.athenaClient();
        assertNotNull(client, "The AthenaClient should not be null");
    }

    @Test
    void testAthenaClientFailure() {
        AthenaConfiguration provider = new AthenaConfiguration() {
            @Override
            public AthenaClient athenaClient() {
                throw new RuntimeException("Failed to create AthenaClient");
            }
        };

        RuntimeException exception = assertThrows(RuntimeException.class, provider::athenaClient);
        assertEquals("Failed to create AthenaClient", exception.getMessage());
    }

    @Test
    void testAthenaServiceBeanSuccessful() {
        AthenaConfiguration provider = new AthenaConfiguration();
        AthenaClient client = provider.athenaClient();

        AthenaServiceImpl service = provider.athenaService(ATHENA_DB, OUTPUT_BUCKET, client);
        assertNotNull(service, "athenaService bean should not be null");
    }

    @Test
    void testHydrationAthenaServiceBeanSuccessful() {
        AthenaConfiguration provider = new AthenaConfiguration();
        AthenaClient client = provider.athenaClient();

        AthenaServiceImpl service = provider.hydrationAthenaService(ATHENA_HYDRATION_DB, HYDRATION_OUTPUT_BUCKET,
                client);
        assertNotNull(service, "hydrationAthenaService bean should not be null");
    }
}
