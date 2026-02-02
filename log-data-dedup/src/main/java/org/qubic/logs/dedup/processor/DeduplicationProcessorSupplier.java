package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.logs.dedup.model.EventLog;

public class DeduplicationProcessorSupplier implements ProcessorSupplier<String, EventLog, String, EventLog> {

    private final String storeName;
    private final long retentionMs; // FIXME remove retention here
    private final MeterRegistry meterRegistry;

    public DeduplicationProcessorSupplier(String storeName, long retentionMs, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.retentionMs = retentionMs;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public Processor<String, EventLog, String, EventLog> get() {
        return new DeduplicationProcessor(storeName, retentionMs, meterRegistry);
    }
}
