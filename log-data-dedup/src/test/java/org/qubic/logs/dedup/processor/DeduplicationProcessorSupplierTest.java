package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DeduplicationProcessorSupplierTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);
    private final String storeName = "test-store";

    @Test
    void get_thenReturnDeduplicationProcessorInstance() {
        DeduplicationProcessorSupplier supplier = new DeduplicationProcessorSupplier(storeName, retention, meterRegistry);
        Processor<String, EventLog, String, EventLog> processor = supplier.get();
        assertThat(processor).isNotNull().isInstanceOf(DeduplicationProcessor.class);
    }

    @Test
    void get_multiple_thenCreateNewInstanceEveryCall() {
        DeduplicationProcessorSupplier supplier = new DeduplicationProcessorSupplier(storeName, retention, meterRegistry);

        Processor<String, EventLog, String, EventLog> first = supplier.get();
        Processor<String, EventLog, String, EventLog> second = supplier.get();

        assertThat(first)
                .isNotNull()
                .isInstanceOf(DeduplicationProcessor.class);
        assertThat(second)
                .isNotNull()
                .isInstanceOf(DeduplicationProcessor.class);
        assertThat(first).isNotSameAs(second);
    }
}
