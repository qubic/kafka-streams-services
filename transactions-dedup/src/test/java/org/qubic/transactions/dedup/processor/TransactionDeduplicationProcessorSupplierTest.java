package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.Transaction;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionDeduplicationProcessorSupplierTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);
    private final String storeName = "test-store";

    @Test
    void get_thenReturnTransactionDeduplicationProcessorInstance() {
        TransactionDeduplicationProcessorSupplier supplier = new TransactionDeduplicationProcessorSupplier(storeName, retention, meterRegistry);
        Processor<String, Transaction, String, Transaction> processor = supplier.get();
        assertThat(processor).isNotNull().isInstanceOf(TransactionDeduplicationProcessor.class);
    }

    @Test
    void get_multiple_thenCreateNewInstanceEveryCall() {
        TransactionDeduplicationProcessorSupplier supplier = new TransactionDeduplicationProcessorSupplier(storeName, retention, meterRegistry);

        Processor<String, Transaction, String, Transaction> first = supplier.get();
        Processor<String, Transaction, String, Transaction> second = supplier.get();

        assertThat(first).isNotNull().isInstanceOf(TransactionDeduplicationProcessor.class);
        assertThat(second).isNotNull().isInstanceOf(TransactionDeduplicationProcessor.class);
        assertThat(first).isNotSameAs(second);
    }
}
