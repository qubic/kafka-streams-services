package org.qubic.tickdata.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.junit.jupiter.api.Test;
import org.qubic.tickdata.dedup.model.TickData;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TickDataDeduplicationProcessorSupplierTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);
    private final String storeName = "test-store";

    @Test
    void get_thenReturnTickDataDeduplicationProcessorInstance() {
        TickDataDeduplicationProcessorSupplier supplier = new TickDataDeduplicationProcessorSupplier(storeName, retention, meterRegistry);
        Processor<String, TickData, String, TickData> processor = supplier.get();
        assertThat(processor).isNotNull().isInstanceOf(TickDataDeduplicationProcessor.class);
    }

    @Test
    void get_multiple_thenCreateNewInstanceEveryCall() {
        TickDataDeduplicationProcessorSupplier supplier = new TickDataDeduplicationProcessorSupplier(storeName, retention, meterRegistry);

        Processor<String, TickData, String, TickData> first = supplier.get();
        Processor<String, TickData, String, TickData> second = supplier.get();

        assertThat(first).isNotNull().isInstanceOf(TickDataDeduplicationProcessor.class);
        assertThat(second).isNotNull().isInstanceOf(TickDataDeduplicationProcessor.class);
        assertThat(first).isNotSameAs(second);
    }
}
