package com.erebelo.springdataaws.config;

import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

@Configuration
public class AthenaConfiguration {

    @Bean
    public AthenaClient athenaClient() {
        return AthenaClient.builder().region(Region.US_EAST_1).credentialsProvider(DefaultCredentialsProvider.create())
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(5))
                        .connectionTimeout(Duration.ofSeconds(5)))
                .build();
    }

    @Bean
    @Primary // Default Athena service
    public AthenaServiceImpl athenaService(@Value("${athena.database.name}") String athenaDBName,
            @Value("${s3.output.bucket.url}") String outputBucketUrl, AthenaClient athenaClient) {
        return new AthenaServiceImpl(athenaClient, athenaDBName, outputBucketUrl);
    }

    @Bean("hydrationAthenaService")
    public AthenaServiceImpl hydrationAthenaService(@Value("${athena.hydration.database.name}") String athenaDBName,
            @Value("${s3.hydration.output.bucket.url}") String outputBucketUrl, AthenaClient athenaClient) {
        return new AthenaServiceImpl(athenaClient, athenaDBName, outputBucketUrl);
    }
}
