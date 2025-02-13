package com.erebelo.springdataaws.config;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

@Configuration
public class AthenaClientConfiguration {

    @Bean
    public AthenaClient athenaClient() {
        return AthenaClient.builder().region(Region.US_EAST_1).credentialsProvider(DefaultCredentialsProvider.create())
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(5))
                        .connectionTimeout(Duration.ofSeconds(5)))
                .build();
    }
}
