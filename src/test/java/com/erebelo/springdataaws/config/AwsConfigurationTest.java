package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class AwsConfigurationTest {

    private AwsConfiguration configuration;

    @BeforeEach
    void setUp() {
        configuration = new AwsConfiguration();
    }

    @Test
    void testAwsCredentialsProviderWithStaticCredentialsSuccessful() {
        String accessKey = "test-access-key";
        String secretKey = "test-secret-key";

        AwsCredentialsProvider provider = configuration.awsCredentialsProvider(accessKey, secretKey);

        assertNotNull(provider, "The AwsCredentialsProvider should not be null");
        assertInstanceOf(StaticCredentialsProvider.class, provider, "Expected a StaticCredentialsProvider instance");

        AwsCredentials credentials = provider.resolveCredentials();
        assertEquals(accessKey, credentials.accessKeyId());
        assertEquals(secretKey, credentials.secretAccessKey());
    }

    @Test
    void testAwsCredentialsProviderWithBlankKeysUsesDefaultProvider() {
        try (MockedStatic<DefaultCredentialsProvider> mockedStatic = mockStatic(DefaultCredentialsProvider.class)) {
            DefaultCredentialsProvider mockProvider = mock(DefaultCredentialsProvider.class);
            mockedStatic.when(DefaultCredentialsProvider::create).thenReturn(mockProvider);

            AwsCredentialsProvider provider = configuration.awsCredentialsProvider(" ", "  ");

            assertNotNull(provider, "Provider should not be null when using default credentials");
            assertEquals(mockProvider, provider, "Should return DefaultCredentialsProvider instance");
            mockedStatic.verify(DefaultCredentialsProvider::create);
        }
    }

    @Test
    void testAwsRegionSuccessful() {
        String regionName = "us-east-2";

        Region region = configuration.awsRegion(regionName);

        assertNotNull(region, "The region should not be null");
        assertEquals(Region.US_EAST_2, region, "Region should match the provided region string");
    }

    @Test
    void testAwsRegionFailureWithInvalidRegion() {
        String invalidRegion = "invalid-region";

        assertThrows(IllegalArgumentException.class, () -> configuration.awsRegion(invalidRegion),
                "Should throw exception for invalid AWS region name");
    }
}
