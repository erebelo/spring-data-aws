package com.erebelo.springdataaws.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

@Configuration
public class AwsConfiguration {

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider(@Value("${aws.access-key}") String accessKey,
            @Value("${aws.secret-key}") String secretKey) {
        if (!accessKey.isBlank() && !secretKey.isBlank()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }

        return DefaultCredentialsProvider.create();
    }

    @Bean
    public Region awsRegion(@Value("${aws.region}") String region) {
        if (Region.regions().stream().noneMatch(r -> r.id().equals(region))) {
            throw new IllegalArgumentException("Invalid AWS region: " + region);
        }
        return Region.of(region);
    }
}
