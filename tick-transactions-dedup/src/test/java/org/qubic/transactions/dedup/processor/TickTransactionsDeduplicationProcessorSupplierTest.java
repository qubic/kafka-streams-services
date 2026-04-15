package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.TickTransactions;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TickTransactionsDeduplicationProcessorSupplierTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);
    private final String storeName = "test-store";

    @Test
    void get_thenReturnTickTransactionsDeduplicationProcessorInstance() {
        TickTransactionsDeduplicationProcessorSupplier supplier = new TickTransactionsDeduplicationProcessorSupplier(storeName, retention, meterRegistry);
        Processor<String, TickTransactions, String, TickTransactions> processor = supplier.get();
        assertThat(processor).isNotNull().isInstanceOf(TickTransactionsDeduplicationProcessor.class);
    }

    @Test
    void get_multiple_thenCreateNewInstanceEveryCall() {
        TickTransactionsDeduplicationProcessorSupplier supplier = new TickTransactionsDeduplicationProcessorSupplier(storeName, retention, meterRegistry);

        Processor<String, TickTransactions, String, TickTransactions> first = supplier.get();
        Processor<String, TickTransactions, String, TickTransactions> second = supplier.get();

        assertThat(first).isNotNull().isInstanceOf(TickTransactionsDeduplicationProcessor.class);
        assertThat(second).isNotNull().isInstanceOf(TickTransactionsDeduplicationProcessor.class);
        assertThat(first).isNotSameAs(second);
    }
}
