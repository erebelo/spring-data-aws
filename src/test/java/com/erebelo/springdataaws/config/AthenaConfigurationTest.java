package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;

class AthenaConfigurationTest {

    private static final String ATHENA_DEFAULT_DB = "db_test";
    private static final String DEFAULT_OUTPUT_BUCKET = "s3://test-output-bucket";
    private static final String DEFAULT_WORKGROUP = "test_wg";
    private static final String ATHENA_HYDRATION_DB = "hydration_db_test";
    private static final String HYDRATION_OUTPUT_BUCKET = "s3://test-hydration-output-bucket";
    private static final String HYDRATION_WORKGROUP = "test_hydration_wg";

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
    void testDefaultAthenaServiceBeanSuccessful() {
        AthenaConfiguration provider = new AthenaConfiguration();
        AthenaClient client = provider.athenaClient();

        AthenaServiceImpl service = provider.defaultAthenaService(client, ATHENA_DEFAULT_DB, DEFAULT_OUTPUT_BUCKET,
                DEFAULT_WORKGROUP);
        assertNotNull(service, "athenaService bean should not be null");
    }

    @Test
    void testHydrationAthenaServiceBeanSuccessful() {
        AthenaConfiguration provider = new AthenaConfiguration();
        AthenaClient client = provider.athenaClient();

        AthenaServiceImpl service = provider.hydrationAthenaService(client, ATHENA_HYDRATION_DB,
                HYDRATION_OUTPUT_BUCKET, HYDRATION_WORKGROUP);
        assertNotNull(service, "hydrationAthenaService bean should not be null");
    }
}
