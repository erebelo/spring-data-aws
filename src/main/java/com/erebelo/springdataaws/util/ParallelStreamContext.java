package com.erebelo.springdataaws.util;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.slf4j.MDC;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

@UtilityClass
public class ParallelStreamContext {

    public static <T> void forEach(Stream<T> stream, Consumer<T> action) {
        long mainThreadId = Thread.currentThread().threadId();
        RequestAttributes contextAttributes = RequestContextHolder.getRequestAttributes();
        Map<String, String> loggingContext = MDC.getCopyOfContextMap();

        stream.parallel().forEach(item -> {
            RequestContextHolder.setRequestAttributes(contextAttributes);

            if (MDC.getCopyOfContextMap() == null && loggingContext != null) {
                MDC.setContextMap(loggingContext);
            }

            try {
                action.accept(item);
            } finally {
                if (Thread.currentThread().threadId() != mainThreadId) {
                    RequestContextHolder.resetRequestAttributes();
                    MDC.clear();
                }
            }
        });
    }
}
