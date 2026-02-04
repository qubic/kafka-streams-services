package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.qubic.logs.dedup.model.EventLog;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
public class DeduplicationProcessor implements Processor<String, EventLog, String, EventLog> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    private ProcessorContext<String, EventLog> context;
    private WindowStore<String, Long> stateStore;

    private Counter processedCounter;
    private Counter duplicateCounter;
    private Counter uniqueCounter;

    public DeduplicationProcessor(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.meterRegistry = meterRegistry;
        this.retention = retention;
    }

    @Override
    public void init(ProcessorContext<String, EventLog> context) {
        this.context = context;
        this.stateStore = context.getStateStore(storeName);

        // Initialize metrics
        this.processedCounter = Counter.builder("dedup.events.processed")
                .description("Total events processed")
                .register(meterRegistry);

        this.duplicateCounter = Counter.builder("dedup.events.duplicate")
                .description("Duplicate events filtered")
                .register(meterRegistry);

        this.uniqueCounter = Counter.builder("dedup.events.unique")
                .description("Unique events forwarded")
                .register(meterRegistry);

        log.info("DeduplicationProcessor initialized for store: [{}].", storeName);
    }

    @Override
    public void process(Record<String, EventLog> record) {
        Record<String, EventLog> result = processRecord(record);
        if (result != null) {
            context.forward(result);
        } // else skip
    }

    public Record<String, EventLog> processRecord(Record<String, EventLog> record) {
        processedCounter.increment();

        EventLog event = record.value();
        Assert.notNull(event, "Received null event.");
        String dedupKey = event.getTickNumber() + ":" + event.getIndex(); // FIXME use epoch + logId first

        Instant recordTime = Instant.ofEpochMilli(record.timestamp());
        // Truncate timestamp to one-minute granularity to enable overwriting within the same minute
        Instant recordKeyTime = recordTime.truncatedTo(ChronoUnit.MINUTES);

        // Check if this key exists in the store (use explicit retention time to allow testing, we could also rely on the stores expiry policy)
        boolean isDuplicate;
        try (WindowStoreIterator<Long> iterator = stateStore.fetch(dedupKey, recordKeyTime.minus(retention), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            // TODO add check for consistency
            isDuplicate = iterator.hasNext();
        }

        // Always store the latest occurrence (truncated to a minute for overwriting similar keys within one minute). The value is irrelevant.
        stateStore.put(dedupKey, record.timestamp(), recordKeyTime.toEpochMilli()); // TODO store consistency check data instead of timestamp

        if (isDuplicate) {
            // Found a duplicate within the retention window
            duplicateCounter.increment();
            if (log.isDebugEnabled()) {
                log.debug("Duplicate found for key: {}", dedupKey);
            }
            return null;
        }

        // Not a duplicate - forward
        uniqueCounter.increment();

        // if record is modified a copy should be forwarded. But as we only filter
        // and do no modifications, it's safe to forward the original record.
        // See ProcessorContext.forward(Record<K,V>) Javadocs.
        return record;
    }

    @Override
    public void close() {
        log.info("DeduplicationProcessor closing");
    }

}