package com.erebelo.springdataaws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.erebelo"})
public class SpringDataAwsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringDataAwsApplication.class, args);
    }

}
