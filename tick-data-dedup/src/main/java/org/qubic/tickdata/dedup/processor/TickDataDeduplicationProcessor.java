package org.qubic.tickdata.dedup.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.qubic.tickdata.dedup.model.TickData;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

@Slf4j
public class TickDataDeduplicationProcessor implements Processor<String, TickData, String, TickData> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    private ProcessorContext<String, TickData> context;
    private WindowStore<String, String> stateStore;

    private Counter processedCounter;
    private Counter duplicateCounter;
    private Counter uniqueCounter;

    public TickDataDeduplicationProcessor(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.retention = retention;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void init(ProcessorContext<String, TickData> context) {
        this.context = context;
        this.stateStore = context.getStateStore(storeName);

        this.processedCounter = Counter.builder("dedup.tickdata.processed")
                .description("Total tick data messages processed")
                .register(meterRegistry);

        this.duplicateCounter = Counter.builder("dedup.tickdata.duplicate")
                .description("Duplicate tick data messages filtered")
                .register(meterRegistry);

        this.uniqueCounter = Counter.builder("dedup.tickdata.unique")
                .description("Unique tick data messages forwarded")
                .register(meterRegistry);

        log.info("TickDataDeduplicationProcessor initialized for store: [{}].", storeName);
    }

    @Override
    public void process(Record<String, TickData> record) {
        processedCounter.increment();

        TickData tickData = record.value();
        Assert.notNull(tickData, "Received null tick data.");

        String dedupKey = String.valueOf(tickData.getTickNumber());
        String dedupValue = tickData.getSignature();

        Instant recordTime = Instant.ofEpochMilli(record.timestamp());
        Instant recordKeyTime = recordTime.truncatedTo(ChronoUnit.MINUTES);

        boolean isDuplicate = false;
        try (WindowStoreIterator<String> iterator = stateStore.fetch(dedupKey, recordKeyTime.minus(retention), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            if (iterator.hasNext()) {
                isDuplicate = true;
                validateDuplicate(iterator, dedupValue, dedupKey);
            }
        }

        stateStore.put(dedupKey, dedupValue, recordKeyTime.toEpochMilli());

        if (isDuplicate) {
            duplicateCounter.increment();
            if (log.isDebugEnabled()) {
                log.debug("Duplicate found for tickNumber: [{}].", dedupKey);
            }
            return;
        }

        uniqueCounter.increment();
        context.forward(record);
    }

    private void validateDuplicate(WindowStoreIterator<String> iterator, String expectedValue, String key) {
        while (iterator.hasNext()) {
            KeyValue<Long, String> entry = iterator.next();
            if (!Objects.equals(entry.value, expectedValue)) {
                log.error("Invalid duplicate value for key [{}]: expected [{}], found [{}].", key, expectedValue, entry.value);
                throw new IllegalStateException(String.format("Invalid duplicate value for key [%s]: expected [%s], found [%s].", key, expectedValue, entry.value));
            }
        }
    }

    @Override
    public void close() {
        log.info("TickDataDeduplicationProcessor closing...");
    }
}
