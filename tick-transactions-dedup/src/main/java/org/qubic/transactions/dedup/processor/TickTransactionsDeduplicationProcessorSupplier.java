package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.transactions.dedup.model.TickTransactions;

import java.time.Duration;

public class TickTransactionsDeduplicationProcessorSupplier implements ProcessorSupplier<String, TickTransactions, String, TickTransactions> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    public TickTransactionsDeduplicationProcessorSupplier(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.meterRegistry = meterRegistry;
        this.retention = retention;
    }

    @Override
    public Processor<String, TickTransactions, String, TickTransactions> get() {
        return new TickTransactionsDeduplicationProcessor(storeName, retention, meterRegistry);
    }

}
