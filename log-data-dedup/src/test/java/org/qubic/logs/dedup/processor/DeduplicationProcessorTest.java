package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DeduplicationProcessorTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);

    private final ProcessorContext<String, EventLog> context = mock();
    private final WindowStore<String, Long> stateStore = mock();
    private final DeduplicationProcessor processor = new DeduplicationProcessor("test-store", retention, metrics);

    @BeforeEach
    void setUp() {
        when(context.getStateStore(anyString())).thenReturn(stateStore);
        processor.init(context);
    }

    @Test
    void process_givenUnique_thenForward() {
        EventLog event = EventLog.builder().tickNumber(100).index(1).build();
        Record<String, EventLog> record = new Record<>("key", event, 1000L);
        WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("100:1"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context).forward(record);
        verify(stateStore).put(eq("100:1"), eq(1000L), anyLong());
        assertThat(metrics.get("dedup.events.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void process_givenDuplicate_thenDoNotForward() {
        EventLog event = EventLog.builder().tickNumber(100).index(1).build();
        Record<String, EventLog> record = new Record<>("key", event, 1000L);
        WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(true);
        when(stateStore.fetch(eq("100:1"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context, never()).forward(any());
        verify(stateStore).put(eq("100:1"), eq(1000L), anyLong());
        assertThat(metrics.get("dedup.events.duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    void processRecord_givenUnique_thenReturnRecord() {
        EventLog event = EventLog.builder().tickNumber(200).index(2).build();
        Record<String, EventLog> record = new Record<>("key", event, 2000L);
        WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("200:2"), any(), any())).thenReturn(iterator);

        Record<String, EventLog> result = processor.processRecord(record);

        assertThat(result).isSameAs(record);
        verify(stateStore).put(eq("200:2"), eq(2000L), anyLong());
    }

    @Test
    void processRecord_givenDuplicate_thenReturnNull() {
        EventLog event = EventLog.builder().tickNumber(200).index(2).build();
        Record<String, EventLog> record = new Record<>("key", event, 2000L);
        WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(true);
        when(stateStore.fetch(eq("200:2"), any(), any())).thenReturn(iterator);

        Record<String, EventLog> result = processor.processRecord(record);

        assertThat(result).isNull();
        verify(stateStore).put(eq("200:2"), eq(2000L), anyLong());
    }
}