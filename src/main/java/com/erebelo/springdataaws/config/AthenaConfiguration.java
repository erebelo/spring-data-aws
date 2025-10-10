package com.erebelo.springdataaws.config;

import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

@Configuration
public class AthenaConfiguration {

    @Bean
    public AthenaClient athenaClient(AwsCredentialsProvider credentialsProvider, Region region) {
        return AthenaClient
                .builder().region(region).credentialsProvider(credentialsProvider).httpClientBuilder(ApacheHttpClient
                        .builder().socketTimeout(Duration.ofSeconds(5)).connectionTimeout(Duration.ofSeconds(5)))
                .build();
    }

    @Bean
    @Primary // Default Athena service
    public AthenaServiceImpl defaultAthenaService(AthenaClient athenaClient,
            @Value("${athena.primary.database}") String athenaDatabase,
            @Value("${s3.primary.output.bucket.url}") String outputBucketUrl,
            @Value("${athena.primary.workgroup}") String workgroup) {
        return new AthenaServiceImpl(athenaClient, athenaDatabase, outputBucketUrl, workgroup);
    }

    @Bean("hydrationAthenaService")
    public AthenaServiceImpl hydrationAthenaService(AthenaClient athenaClient,
            @Value("${athena.hydration.database}") String athenaDatabase,
            @Value("${s3.hydration.output.bucket.url}") String outputBucketUrl,
            @Value("${athena.hydration.workgroup}") String workgroup) {
        return new AthenaServiceImpl(athenaClient, athenaDatabase, outputBucketUrl, workgroup);
    }
}
