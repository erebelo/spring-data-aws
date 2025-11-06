// package com.erebelo.springdataaws.config;
//
// import static org.assertj.core.api.Assertions.assertThat;
//
// import java.util.concurrent.Executor;
// import org.junit.jupiter.api.Test;
// import org.springframework.boot.test.context.runner.ApplicationContextRunner;
// import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
//
// class SpringDataAwsConfigurationTest {
//
// private final ApplicationContextRunner contextRunner = new
// ApplicationContextRunner()
// .withUserConfiguration(SpringDataAwsConfiguration.class);
//
// @Test
// void testAsyncTaskExecutorConfiguration() {
// contextRunner.run(context -> {
// assertThat(context).hasSingleBean(Executor.class);
//
// Executor executorBean = context.getBean(Executor.class);
// assertThat(executorBean).isInstanceOf(ThreadPoolTaskExecutor.class);
//
// ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) executorBean;
//
// assertThat(executor.getCorePoolSize()).isEqualTo(4);
// assertThat(executor.getMaxPoolSize()).isEqualTo(8);
// assertThat(executor.getThreadPoolExecutor().getQueue().remainingCapacity()).isEqualTo(100);
// assertThat(executor.getThreadNamePrefix()).isEqualTo("Async-Thread");
// assertThat(executor.getThreadPoolExecutor()).isNotNull();
// });
// }
// }
