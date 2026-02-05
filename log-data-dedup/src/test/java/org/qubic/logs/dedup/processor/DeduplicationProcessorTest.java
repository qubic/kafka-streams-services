package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class DeduplicationProcessorTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);

    private final ProcessorContext<String, EventLog> context = mock();
    private final WindowStore<String, String> stateStore = mock();
    private final DeduplicationProcessor processor = new DeduplicationProcessor("test-store", retention, metrics);

    @BeforeEach
    void setUp() {
        when(context.getStateStore(anyString())).thenReturn(stateStore);
        processor.init(context);
    }

    @Test
    void process_givenUnique_thenForward() {
        EventLog event = EventLog.builder().epoch(123).logId(2).tickNumber(100).index(1).type(3).logDigest("digest").build();
        Record<String, EventLog> record = new Record<>("key", event, 1000L);
        WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("123:2"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context).forward(record);
        verify(stateStore).put(eq("123:2"), eq("100:1:3:digest"), anyLong());
        assertThat(metrics.get("dedup.events.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void process_givenDuplicate_thenDoNotForward() {
        EventLog event = EventLog.builder().epoch(123).logId(2).tickNumber(100).index(1).type(3).logDigest("digest").build();
        Record<String, EventLog> record = new Record<>("key", event, 1000L);
        WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, "100:1:3:digest"));
        when(stateStore.fetch(eq("123:2"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context, never()).forward(any());
        verify(stateStore).put(eq("123:2"), eq("100:1:3:digest"), anyLong());
        assertThat(metrics.get("dedup.events.duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    void processRecord_givenUnique_thenReturnRecord() {
        EventLog event = EventLog.builder().epoch(123).logId(2).build();
        Record<String, EventLog> record = new Record<>("key", event, 2000L);
        WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("123:2"), any(), any())).thenReturn(iterator);

        Record<String, EventLog> result = processor.processRecord(record);
        assertThat(result).isSameAs(record);
    }

    @Test
    void processRecord_givenDuplicate_thenReturnNull() {
        EventLog event = EventLog.builder().epoch(123).logId(2).build();
        Record<String, EventLog> record = new Record<>("key", event, 2000L);
        WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, "0:0:0:null"));
        when(stateStore.fetch(eq("123:2"), any(), any())).thenReturn(iterator);

        Record<String, EventLog> result = processor.processRecord(record);
        assertThat(result).isNull();
    }

    @Test
    void process_givenDuplicateWithDifferentDedupValue_thenThrows() {
        // Same dedupKey (epoch:logId) but different dedupValue (index differs)
        EventLog event = EventLog.builder()
                .epoch(123).logId(2)
                .tickNumber(100).index(1).type(3).logDigest("digest")
                .build();
        Record<String, EventLog> record = new Record<>("key", event, 1000L);

        WindowStoreIterator<String> iterator = mock();
        // First hasNext() -> true (isDuplicate), then loop: true, then false
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        // Provide a mismatching dedupValue (index differs: 2 instead of 1)
        when(iterator.next()).thenReturn(KeyValue.pair(666L, "100:2:3:digest"));
        when(stateStore.fetch(eq("123:2"), any(), any())).thenReturn(iterator);

        assertThatThrownBy(() -> processor.process(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid duplicate");

        // Ensure we did not write to the store after detecting inconsistency
        verify(stateStore, never()).put(any(), any(), anyLong());
    }

    @Test
    void process_givenIncreasingTickNumber_thenIncrementTickCounter() {
        WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(anyString(), any(), any())).thenReturn(iterator);

        // First event with tick 100
        EventLog event1 = EventLog.builder().epoch(123).logId(1).tickNumber(100).build();
        processor.process(new Record<>("key1", event1, 1000L));
        assertThat(metrics.get("dedup.ticks.processed").counter().count()).isEqualTo(1.0);

        // Second event with the same tick 100
        EventLog event2 = EventLog.builder().epoch(123).logId(2).tickNumber(100).build();
        processor.process(new Record<>("key2", event2, 2000L));
        assertThat(metrics.get("dedup.ticks.processed").counter().count()).isEqualTo(1.0);

        // Third event with tick 101
        EventLog event3 = EventLog.builder().epoch(123).logId(3).tickNumber(101).build();
        processor.process(new Record<>("key3", event3, 3000L));
        assertThat(metrics.get("dedup.ticks.processed").counter().count()).isEqualTo(2.0);

        // Fourth event with lower tick 99 (should not happen in the real world normally, but let's test)
        EventLog event4 = EventLog.builder().epoch(123).logId(4).tickNumber(99).build();
        processor.process(new Record<>("key4", event4, 4000L));
        assertThat(metrics.get("dedup.ticks.processed").counter().count()).isEqualTo(2.0);
    }
}