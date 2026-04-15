package org.qubic.tickdata.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.tickdata.dedup.model.TickData;

import java.time.Duration;

public class TickDataDeduplicationProcessorSupplier implements ProcessorSupplier<String, TickData, String, TickData> {

    private final String storeName;
    private final Duration retention;
    private final MeterRegistry meterRegistry;

    public TickDataDeduplicationProcessorSupplier(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.retention = retention;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public Processor<String, TickData, String, TickData> get() {
        return new TickDataDeduplicationProcessor(storeName, retention, meterRegistry);
    }
}
