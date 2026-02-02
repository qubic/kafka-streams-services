package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DeduplicationProcessorTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final String storeName = "test-store";
    private final long retentionMs = 1000;
    private final DeduplicationProcessor processor = new DeduplicationProcessor(storeName, retentionMs, metrics);

    private MockProcessorContext<String, EventLog> context;
    private KeyValueStore<String, Long> stateStore;

    @BeforeEach
    void setUp() {
        context = new MockProcessorContext<>();
        stateStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                org.apache.kafka.common.serialization.Serdes.String(),
                org.apache.kafka.common.serialization.Serdes.Long()
        ).withLoggingDisabled().build();

        stateStore.init(context.getStateStoreContext(), stateStore);
        context.addStateStore(stateStore);
        processor.init(context);
    }

    @Test
    void shouldForwardNewRecord() {
        EventLog event = EventLog.builder()
                .tick(42)
                .logId(101)
                .logDigest("digest-1")
                .build();

        Record<String, EventLog> record = new Record<>("key", event, System.currentTimeMillis());

        processor.process(record);

        List<MockProcessorContext.CapturedForward<? extends String, ? extends EventLog>> forwarded = context.forwarded();
        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.getFirst().record().value()).isEqualTo(event);

        // Verify state store
        String dedupKey = event.getTick() + ":" + event.getLogId();
        assertThat(stateStore.get(dedupKey)).isEqualTo(record.timestamp());
    }

    @Test
    void shouldNotForwardDuplicateRecord() {
        EventLog event = EventLog.builder()
                .tick(42)
                .logId(101)
                .logDigest("digest-1")
                .build();

        long timestamp = System.currentTimeMillis();
        Record<String, EventLog> record1 = new Record<>("key", event, timestamp);
        Record<String, EventLog> record2 = new Record<>("key", event, timestamp + 500); // within 1000ms retention

        // Process first record
        processor.process(record1);
        assertThat(context.forwarded()).hasSize(1);

        // Process duplicate record
        processor.process(record2);

        // Still should have only 1 forwarded record
        List<MockProcessorContext.CapturedForward<? extends String, ? extends EventLog>> forwarded = context.forwarded();
        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.getFirst().record().value()).isEqualTo(event);

        // Verify metrics
        assertThat(metrics.get("dedup.events.processed").counter().count()).isEqualTo(2.0);
        assertThat(metrics.get("dedup.events.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.events.unique").counter().count()).isEqualTo(1.0);
    }
}