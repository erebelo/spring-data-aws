package com.erebelo.springdataaws.util;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.slf4j.MDC;

@UtilityClass
public class ParallelStreamContext {

    public static <T> void forEach(Stream<T> stream, Consumer<T> action) {
        Map<String, String> loggingContext = MDC.getCopyOfContextMap();

        stream.parallel().forEach(item -> {
            // Set MDC context only if it hasn't been set for the current thread
            if (MDC.getCopyOfContextMap() == null && loggingContext != null) {
                MDC.setContextMap(loggingContext);
            }

            action.accept(item);
        });
    }
}
