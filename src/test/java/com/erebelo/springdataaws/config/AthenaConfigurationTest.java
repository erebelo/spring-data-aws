package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

class AthenaConfigurationTest {

    private static final String ATHENA_DB = "_db_test";
    private static final String OUTPUT_BUCKET = "s3://test-output-bucket";
    private static final String WORKGROUP = "test_wg";

    private AthenaConfiguration configuration;
    private AwsCredentialsProvider credentialsProvider;
    private Region region;

    @BeforeEach
    void setUp() {
        configuration = new AthenaConfiguration();
        credentialsProvider = mock(AwsCredentialsProvider.class);
        region = Region.US_EAST_2;
    }

    @Test
    void testAthenaClientSuccessful() {
        AthenaClient client = configuration.athenaClient(credentialsProvider, region);

        assertNotNull(client, "The AthenaClient should not be null");
    }

    @Test
    void testAthenaClientFailure() {
        AthenaConfiguration failingConfig = new AthenaConfiguration() {
            @Override
            public AthenaClient athenaClient(AwsCredentialsProvider credentialsProvider, Region region) {
                throw new RuntimeException("Failed to create AthenaClient");
            }
        };

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> failingConfig.athenaClient(credentialsProvider, region));

        assertEquals("Failed to create AthenaClient", exception.getMessage());
    }

    @Test
    void testDefaultAthenaServiceBeanSuccessful() {
        AthenaClient client = configuration.athenaClient(credentialsProvider, region);

        AthenaServiceImpl service = configuration.defaultAthenaService(client, ATHENA_DB, OUTPUT_BUCKET, WORKGROUP);

        assertNotNull(service, "athenaService bean should not be null");
    }
}
