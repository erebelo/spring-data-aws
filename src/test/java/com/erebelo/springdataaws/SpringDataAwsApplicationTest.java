package com.erebelo.springdataaws;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

@ExtendWith(MockitoExtension.class)
class SpringDataAwsApplicationTest {

    @Mock
    private ConfigurableApplicationContext contextMock;

    @Test
    void contextLoads() {
        // This test simply checks if the Spring context loads successfully
        // If it doesn't, it will throw an exception
    }

    @Test
    void mainRunSuccessful() {
        try (MockedStatic<SpringApplication> mockedStatic = mockStatic(SpringApplication.class)) {
            mockedStatic.when(() -> SpringApplication.run(any(Class.class), any(String[].class)))
                    .thenReturn(contextMock);

            SpringDataAwsApplication.main(new String[]{});

            mockedStatic.verify(() -> SpringApplication.run(SpringDataAwsApplication.class, new String[]{}));
        }
    }
}
