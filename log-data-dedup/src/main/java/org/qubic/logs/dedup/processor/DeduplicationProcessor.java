package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.qubic.logs.dedup.model.EventLog;

@Slf4j
public class DeduplicationProcessor implements Processor<String, EventLog, String, EventLog> {

    private final String storeName;
    private final long retentionMs;
    private final MeterRegistry meterRegistry;

    private ProcessorContext<String, EventLog> context;
    private KeyValueStore<String, Long> stateStore;

    private Counter processedCounter;
    private Counter duplicateCounter;
    private Counter uniqueCounter;

    public DeduplicationProcessor(String storeName, long retentionMs, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.retentionMs = retentionMs;
        this.meterRegistry = meterRegistry;
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

        log.info("DeduplicationProcessor initialized for store: {}, retention: {}ms", storeName, retentionMs);
    }

    @Override
    public void process(Record<String, EventLog> record) {
        Record<String, EventLog> result = processRecord(record);
        if (result != null) {
            context.forward(result);
        } else {
            duplicateCounter.increment();
            if (log.isDebugEnabled()) {
                log.debug("Duplicate event filtered: {}.", record);
            }
        }
    }

    public Record<String, EventLog> processRecord(Record<String, EventLog> record) {
        processedCounter.increment();

        // FIXME the following logic does not work! It would fill the store up.

        // TODO we need a time based storage here with expiry instead of storing timestamps
        // maybe we can use Stores.persistentWindowStore

        EventLog event = record.value();

        if (event == null || event.getLogDigest() == null) {
            log.warn("Received null event or null logDigest, dropping");
            return null;
        }

        String dedupKey = event.getTick() + ":" + event.getLogId(); // FIXME use correct deduplication key
        Long lastSeenTimestamp = stateStore.get(dedupKey);

        long currentTimestamp = record.timestamp();
        long expiryTimestamp = currentTimestamp - retentionMs;

        if (lastSeenTimestamp == null || lastSeenTimestamp < expiryTimestamp) {
            // Not a duplicate (or expired entry)
            stateStore.put(dedupKey, currentTimestamp);
            uniqueCounter.increment();

            if (lastSeenTimestamp != null && log.isDebugEnabled()) {
                log.debug("Expired entry refreshed for logDigest: {}", dedupKey);
            }

            // if record is modified a copy should be forwarded. But as we only filter
            // and do no modifications, it's safe to forward the original record.
            // See ProcessorContext.forward(Record<K,V>) Javadocs.
            return record;

        } else {
            return null;
        }
    }

    @Override
    public void close() {
        log.info("DeduplicationProcessor closing");
    }

}