package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.logs.dedup.model.EventLog;

import java.time.Duration;

public class DeduplicationProcessorSupplier implements ProcessorSupplier<String, EventLog, String, EventLog> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    public DeduplicationProcessorSupplier(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.meterRegistry = meterRegistry;
        this.retention = retention;
    }

    @Override
    public Processor<String, EventLog, String, EventLog> get() {
        return new DeduplicationProcessor(storeName, retention, meterRegistry);
    }
}
