package org.qubic.tickdata.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.tickdata.dedup.model.TickData;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class TickDataDeduplicationProcessorTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);

    private final ProcessorContext<String, TickData> context = mock();
    private final WindowStore<String, String> stateStore = mock();
    private final TickDataDeduplicationProcessor processor = new TickDataDeduplicationProcessor("test-store", retention, metrics);

    @BeforeEach
    void setUp() {
        when(context.getStateStore(anyString())).thenReturn(stateStore);
        processor.init(context);
    }

    @Test
    @SuppressWarnings("unchecked")
    void process_givenUnique_thenForward() {
        TickData tickData = TickData.builder()
                .epoch(209L)
                .tickNumber(49485485L)
                .signature("sig123")
                .transactionHashes(List.of("h1", "h2"))
                .build();
        Record<String, TickData> record = new Record<>("key", tickData, 1000L);

        final WindowStoreIterator<String> iterator = mock(WindowStoreIterator.class);
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("49485485"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context).forward(record);
        verify(stateStore).put(eq("49485485"), eq("sig123"), anyLong());
        assertThat(metrics.get("dedup.messages.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void process_givenDuplicate_thenDoNotForward() {
        TickData tickData = TickData.builder()
                .epoch(209L)
                .tickNumber(49485485L)
                .signature("sig123")
                .build();
        Record<String, TickData> record = new Record<>("key", tickData, 1000L);

        final WindowStoreIterator<String> iterator = mock(WindowStoreIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, "sig123"));
        when(stateStore.fetch(eq("49485485"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context, never()).forward(any());
        verify(stateStore).put(eq("49485485"), eq("sig123"), anyLong());
        assertThat(metrics.get("dedup.messages.duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void process_givenDuplicateWithDifferentValue_thenThrows() {
        TickData tickData = TickData.builder()
                .epoch(209L)
                .tickNumber(49485485L)
                .signature("sig123")
                .build();
        Record<String, TickData> record = new Record<>("key", tickData, 1000L);

        final WindowStoreIterator<String> iterator = mock(WindowStoreIterator.class);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, "wrong-sig"));
        when(stateStore.fetch(eq("49485485"), any(), any())).thenReturn(iterator);

        assertThatThrownBy(() -> processor.process(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid duplicate value");

        // In current implementation, put is called AFTER fetch, but if exception is thrown in validateDuplicate, put won't be called.
        verify(stateStore, never()).put(any(), any(), anyLong());
    }
}
