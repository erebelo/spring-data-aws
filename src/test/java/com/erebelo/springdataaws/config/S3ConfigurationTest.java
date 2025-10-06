package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class S3ConfigurationTest {

    private S3Configuration configuration;
    private AwsCredentialsProvider credentialsProvider;
    private Region region;

    @BeforeEach
    void setUp() {
        configuration = new S3Configuration();
        credentialsProvider = mock(AwsCredentialsProvider.class);
        region = Region.US_EAST_2;
    }

    @Test
    void testS3ClientSuccessful() {
        S3Client client = configuration.s3Client(credentialsProvider, region);

        assertNotNull(client, "The S3Client should not be null");
    }

    @Test
    void testS3ClientFailure() {
        S3Configuration failingConfig = new S3Configuration() {
            @Override
            public S3Client s3Client(AwsCredentialsProvider credentialsProvider, Region region) {
                throw new RuntimeException("Failed to create S3Client");
            }
        };

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> failingConfig.s3Client(credentialsProvider, region));

        assertEquals("Failed to create S3Client", exception.getMessage());
    }
}
