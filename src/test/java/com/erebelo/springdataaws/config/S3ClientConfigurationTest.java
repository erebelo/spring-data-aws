package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

class S3ClientConfigurationTest {

    @Test
    void testS3ClientSuccessful() {
        S3ClientConfiguration provider = new S3ClientConfiguration();
        S3Client client = provider.s3Client();
        assertNotNull(client, "The S3Client should not be null");
    }

    @Test
    void testS3ClientFailure() {
        S3ClientConfiguration provider = new S3ClientConfiguration() {
            @Override
            public S3Client s3Client() {
                throw new RuntimeException("Failed to create S3Client");
            }
        };

        Exception exception = assertThrows(RuntimeException.class, provider::s3Client);
        assertEquals("Failed to create S3Client", exception.getMessage());
    }
}
