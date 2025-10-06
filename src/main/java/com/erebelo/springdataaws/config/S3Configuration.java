package com.erebelo.springdataaws.config;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3Configuration {

    @Bean
    public S3Client s3Client(AwsCredentialsProvider credentialsProvider, Region region) {
        return S3Client
                .builder().region(region).credentialsProvider(credentialsProvider).httpClientBuilder(ApacheHttpClient
                        .builder().socketTimeout(Duration.ofSeconds(30)).connectionTimeout(Duration.ofSeconds(5)))
                .build();
    }
}
