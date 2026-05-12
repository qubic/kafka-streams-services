package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.transactions.dedup.model.Transaction;

import java.time.Duration;

public class TransactionDeduplicationProcessorSupplier implements ProcessorSupplier<String, Transaction, String, Transaction> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    public TransactionDeduplicationProcessorSupplier(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.meterRegistry = meterRegistry;
        this.retention = retention;
    }

    @Override
    public Processor<String, Transaction, String, Transaction> get() {
        return new TransactionDeduplicationProcessor(storeName, retention, meterRegistry);
    }

}
