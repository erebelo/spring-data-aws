package com.erebelo.springdataaws.config;

import com.erebelo.springdataaws.service.impl.AthenaServiceImpl;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    public AthenaServiceImpl defaultAthenaService(AthenaClient athenaClient,
            @Value("${athena.database}") String athenaDatabase,
            @Value("${s3.output.bucket.url}") String outputBucketUrl, @Value("${athena.workgroup}") String workgroup) {
        return new AthenaServiceImpl(athenaClient, athenaDatabase, outputBucketUrl, workgroup);
    }
}
