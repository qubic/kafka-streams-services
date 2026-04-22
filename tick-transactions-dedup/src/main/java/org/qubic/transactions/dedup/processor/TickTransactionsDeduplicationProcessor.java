package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.qubic.transactions.dedup.model.TickTransactions;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
public class TickTransactionsDeduplicationProcessor implements Processor<String, TickTransactions, String, TickTransactions> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    private ProcessorContext<String, TickTransactions> context;
    private WindowStore<String, Long> stateStore;

    private Counter processedCounter;
    private Counter duplicateCounter;
    private Counter uniqueCounter;
    private Counter tickCounter;

    public TickTransactionsDeduplicationProcessor(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.retention = retention;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void init(ProcessorContext<String, TickTransactions> context) {
        this.context = context;
        this.stateStore = context.getStateStore(storeName);

        this.processedCounter = Counter.builder("dedup.messages.processed")
                .tag("type", "tick-transactions")
                .description("Total messages processed")
                .register(meterRegistry);

        this.duplicateCounter = Counter.builder("dedup.messages.duplicate")
                .tag("type", "tick-transactions")
                .description("Duplicate messages filtered")
                .register(meterRegistry);

        this.uniqueCounter = Counter.builder("dedup.messages.unique")
                .tag("type", "tick-transactions")
                .description("Unique messages forwarded")
                .register(meterRegistry);

        this.tickCounter = Counter.builder("dedup.ticks.processed")
                .tag("type", "tick-transactions")
                .description("Total ticks processed")
                .register(meterRegistry);

        log.info("TickTransactionsDeduplicationProcessor initialized for store: [{}].", storeName);
    }

    @Override
    public void process(Record<String, TickTransactions> record) {
        processedCounter.increment();
        tickCounter.increment();

        TickTransactions tickTransactions = record.value();
        Assert.notNull(tickTransactions, "Received null tick transactions.");

        String dedupKey = String.valueOf(tickTransactions.getTickNumber());
        long dedupValue = CollectionUtils.size(tickTransactions.getTransactions());

        Instant recordTime = Instant.ofEpochMilli(record.timestamp());
        Instant recordKeyTime = recordTime.truncatedTo(ChronoUnit.MINUTES);

        boolean isDuplicate = false;
        try (WindowStoreIterator<Long> iterator = stateStore.fetch(dedupKey, recordKeyTime.minus(retention), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            if (iterator.hasNext()) {
                isDuplicate = true;
                validateDuplicate(iterator, dedupValue, dedupKey);
            }
        }

        if (isDuplicate) {
            duplicateCounter.increment();
            if (log.isDebugEnabled()) {
                log.debug("Duplicate found for tickNumber: [{}].", dedupKey);
            }
            return;
        } else {
            stateStore.put(dedupKey, dedupValue, recordKeyTime.toEpochMilli());
        }

        uniqueCounter.increment();
        context.forward(record);
    }

    private void validateDuplicate(WindowStoreIterator<Long> iterator, long expectedValue, String key) {
        while (iterator.hasNext()) {
            KeyValue<Long, Long> entry = iterator.next();
            if (entry.value != expectedValue) {
                log.error("Invalid duplicate value for key [{}]: expected [{}], found [{}].", key, expectedValue, entry.value);
                // In this case we might still want to throw an exception or just log it.
                // Following event-logs-dedup style which throws IllegalStateException.
                throw new IllegalStateException(String.format("Invalid duplicate value for key [%s]: expected [%d], found [%d].", key, expectedValue, entry.value));
            }
        }
    }

    @Override
    public void close() {
        log.info("TickTransactionsDeduplicationProcessor closing...");
    }
}
