package com.erebelo.springdataaws.config;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

@Configuration
public class AsyncExecutorConfig {

    /*
     * Dedicated executor for manual asynchronous hydration triggered via
     * CompletableFuture. By declaring it as a separate bean, we ensure that manual
     * triggers execute independently of scheduled tasks and do not compete with
     * them for threads.
     */
    @Bean
    public ThreadPoolTaskExecutor defaultAsyncTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("Def-Executor-");
        executor.setTaskDecorator(new ContextCopyingTaskDecorator());
        executor.initialize();
        return executor;
    }

    static class ContextCopyingTaskDecorator implements TaskDecorator {
        @Override
        public @NonNull Runnable decorate(@NonNull Runnable runnable) {
            final RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
            final Map<String, String> loggingContext = MDC.getCopyOfContextMap();
            final Map<String, String> headers = HeaderContextHolder.get();

            return () -> {
                try {
                    // Restore web request context (if this async call originated from an HTTP
                    // request)
                    if (requestAttributes != null) {
                        RequestContextHolder.setRequestAttributes(requestAttributes);
                    }

                    // Restore MDC context for logging
                    if (loggingContext != null) {
                        MDC.setContextMap(loggingContext);
                    }

                    // Reuse or initialize header context
                    Map<String, String> headerContext = headers != null ? new HashMap<>(headers) : new HashMap<>();

                    // Add unique async task ID
                    String asyncTaskId = UUID.randomUUID().toString();
                    headerContext.put("asyncTaskId", asyncTaskId);
                    HeaderContextHolder.set(headerContext);

                    // Also add to MDC for log correlation
                    MDC.put("asyncTaskId", asyncTaskId);

                    runnable.run();
                } finally {
                    RequestContextHolder.resetRequestAttributes();
                    HeaderContextHolder.clear();
                    MDC.clear();
                }
            };
        }
    }

    static final class HeaderContextHolder {
        private static final ThreadLocal<Map<String, String>> CONTEXT = new ThreadLocal<>();

        private HeaderContextHolder() {
        }

        static void set(Map<String, String> headers) {
            CONTEXT.set(headers);
        }

        static Map<String, String> get() {
            return CONTEXT.get();
        }

        static void clear() {
            CONTEXT.remove();
        }
    }
}
