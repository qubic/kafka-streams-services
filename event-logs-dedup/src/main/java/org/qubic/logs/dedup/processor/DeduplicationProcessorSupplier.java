package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.Range;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.logs.dedup.model.EventLog;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DeduplicationProcessorSupplier implements ProcessorSupplier<String, EventLog, String, EventLog> {

    private final String storeName;
    private final Map<Long, List<Range<Long>>> ignoredKeys;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    public DeduplicationProcessorSupplier(String storeName, Map<Long, List<Range<Long>>> ignoredKeys, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.ignoredKeys = ignoredKeys;
        this.meterRegistry = meterRegistry;
        this.retention = retention;
    }

    @Override
    public Processor<String, EventLog, String, EventLog> get() {
        return new DeduplicationProcessor(storeName, ignoredKeys, retention, meterRegistry);
    }

}
