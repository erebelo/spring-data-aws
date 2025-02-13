package com.erebelo.springdataaws.config;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3ClientConfiguration {

    @Bean
    public S3Client s3Client() {
        return S3Client.builder().region(Region.US_EAST_1).credentialsProvider(DefaultCredentialsProvider.create())
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(5)))
                .build();
    }
}
