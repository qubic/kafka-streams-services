package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.qubic.transactions.dedup.model.Transaction;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
public class TransactionDeduplicationProcessor implements Processor<String, Transaction, String, Transaction> {

    private final String storeName;
    private final MeterRegistry meterRegistry;
    private final Duration retention;

    private ProcessorContext<String, Transaction> context;
    private WindowStore<String, String> stateStore;

    private Counter processedCounter;
    private Counter duplicateCounter;
    private Counter uniqueCounter;
    private Counter tickCounter;

    public TransactionDeduplicationProcessor(String storeName, Duration retention, MeterRegistry meterRegistry) {
        this.storeName = storeName;
        this.retention = retention;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void init(ProcessorContext<String, Transaction> context) {
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
    public void process(Record<String, Transaction> record) {
        processedCounter.increment();
        tickCounter.increment();

        Transaction transaction = record.value();
        Assert.notNull(transaction, "Received null transaction.");

        // shorten hash and key a bit. key collision should be very unlikely within one tick
        String dedupKey = String.format("%d:%s", transaction.getTickNumber(), StringUtils.substring(transaction.getHash(), 0, 16));
        String dedupValue = StringUtils.substring(transaction.getSignature(), 0, 8); // only take the first 8 characters (6 bytes)

        Instant recordTime = Instant.ofEpochMilli(record.timestamp());
        Instant recordKeyTime = recordTime.truncatedTo(ChronoUnit.MINUTES);

        boolean isDuplicate = false;
        try (WindowStoreIterator<String> iterator = stateStore.fetch(dedupKey, recordKeyTime.minus(retention), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            if (iterator.hasNext()) {
                isDuplicate = true;
                validateDuplicate(iterator, dedupValue, dedupKey);
            }
        }

        if (isDuplicate) {
            duplicateCounter.increment();
            if (log.isDebugEnabled()) {
                log.debug("Duplicate found for hash: [{}].", dedupKey);
            }
            return;
        } else {
            stateStore.put(dedupKey, dedupValue, recordKeyTime.toEpochMilli());
        }

        uniqueCounter.increment();
        context.forward(record);
    }

    private void validateDuplicate(WindowStoreIterator<String> iterator, String expectedValue, String key) {
        while (iterator.hasNext()) {
            KeyValue<Long, String> entry = iterator.next();
            if (!Strings.CS.equals(entry.value, expectedValue)) {
                log.error("Invalid duplicate value for key [{}]: expected [{}], found [{}].", key, expectedValue, entry.value);
                // In this case we might still want to throw an exception or just log it.
                // Following event-logs-dedup style which throws IllegalStateException.
                throw new IllegalStateException(String.format("Invalid duplicate value for key [%s]: expected [%s], found [%s].", key, expectedValue, entry.value));
            }
        }
    }

    @Override
    public void close() {
        log.info("TickTransactionsDeduplicationProcessor closing...");
    }
}
