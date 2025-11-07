package com.erebelo.springdataaws.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

@ExtendWith(MockitoExtension.class)
class AsyncExecutorConfigurationTest {

    @InjectMocks
    private AsyncExecutorConfiguration config;

    @Test
    void testDefaultAsyncTaskExecutorSuccessful() throws Exception {
        ThreadPoolTaskExecutor executor = config.defaultAsyncTaskExecutor();

        assertNotNull(executor);
        assertEquals(10, executor.getCorePoolSize());
        assertEquals(20, executor.getMaxPoolSize());
        assertEquals(500, executor.getQueueCapacity());
        assertEquals("Def-Executor-", executor.getThreadNamePrefix());

        TaskDecorator decorator = getPrivateTaskDecorator(executor);
        assertNotNull(decorator, "TaskDecorator should be set");
        assertInstanceOf(AsyncExecutorConfiguration.ContextCopyingTaskDecorator.class, decorator);
    }

    @Test
    void testContextCopyingTaskDecoratorExecutesWithContext() {
        Map<String, String> fakeHeaders = new HashMap<>();
        fakeHeaders.put("X-Test", "123");

        Map<String, String> fakeMdcContext = new HashMap<>();
        fakeMdcContext.put("traceId", "abc-123");

        RequestAttributes mockAttributes = mock(RequestAttributes.class);

        try (MockedStatic<AsyncExecutorConfiguration.HeaderContextHolder> headerMock = mockStatic(
                AsyncExecutorConfiguration.HeaderContextHolder.class);
                MockedStatic<MDC> mdcMock = mockStatic(MDC.class);
                MockedStatic<RequestContextHolder> requestContextMock = mockStatic(RequestContextHolder.class)) {

            // Mock static method returns
            headerMock.when(AsyncExecutorConfiguration.HeaderContextHolder::get).thenReturn(fakeHeaders);
            mdcMock.when(MDC::getCopyOfContextMap).thenReturn(fakeMdcContext);
            requestContextMock.when(RequestContextHolder::getRequestAttributes).thenReturn(mockAttributes);

            Runnable runnable = mock(Runnable.class);
            TaskDecorator decorator = new AsyncExecutorConfiguration.ContextCopyingTaskDecorator();

            Runnable decorated = decorator.decorate(runnable);
            assertNotNull(decorated);

            decorated.run();

            // Assert - verify restoration of contexts before execution
            requestContextMock.verify(() -> RequestContextHolder.setRequestAttributes(mockAttributes));
            mdcMock.verify(() -> MDC.setContextMap(fakeMdcContext));
            headerMock.verify(() -> AsyncExecutorConfiguration.HeaderContextHolder.set(anyMap()));

            // Verify asyncTaskId was added (not null)
            mdcMock.verify(() -> MDC.put(eq("asyncTaskId"), notNull()));

            // Verify runnable executed
            verify(runnable, times(1)).run();

            // Verify cleanup after execution
            requestContextMock.verify(RequestContextHolder::resetRequestAttributes);
            headerMock.verify(AsyncExecutorConfiguration.HeaderContextHolder::clear);
            mdcMock.verify(MDC::clear);
        }
    }

    private TaskDecorator getPrivateTaskDecorator(ThreadPoolTaskExecutor executor) throws Exception {
        Field field = ThreadPoolTaskExecutor.class.getDeclaredField("taskDecorator");
        field.setAccessible(true);
        return (TaskDecorator) field.get(executor);
    }
}
