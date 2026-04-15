package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.TickTransactions;
import org.qubic.transactions.dedup.model.Transaction;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TickTransactionsDeduplicationProcessorTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);

    private final ProcessorContext<String, TickTransactions> context = mock();
    private final WindowStore<String, Long> stateStore = mock();
    private final TickTransactionsDeduplicationProcessor processor = new TickTransactionsDeduplicationProcessor("test-store", retention, metrics);

    @BeforeEach
    void setUp() {
        when(context.getStateStore(anyString())).thenReturn(stateStore);
        processor.init(context);
    }

    @Test
    void process_givenUnique_thenForward() {
        TickTransactions tickTransactions = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .transactions(List.of(new Transaction()))
                .build();
        Record<String, TickTransactions> record = new Record<>("key", tickTransactions, 1000L);
        
        final WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("49189280"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context).forward(record);
        verify(stateStore).put(eq("49189280"), eq(1L), anyLong());
        assertThat(metrics.get("dedup.transactions.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void process_givenDuplicate_thenDoNotForward() {
        TickTransactions tickTransactions = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .transactions(List.of(new Transaction(), new Transaction()))
                .build();
        Record<String, TickTransactions> record = new Record<>("key", tickTransactions, 1000L);

        final WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, 49189280L));
        when(stateStore.fetch(eq("49189280"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context, never()).forward(any());
        verify(stateStore).put(eq("49189280"), eq(2L), anyLong());
        assertThat(metrics.get("dedup.transactions.duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    void process_givenDuplicateWithDifferentValue_thenThrows() {
        TickTransactions tickTransactions = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .build();
        Record<String, TickTransactions> record = new Record<>("key", tickTransactions, 1000L);

        final WindowStoreIterator<Long> iterator = mock();
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, 12345L)); // wrong value
        when(stateStore.fetch(eq("49189280"), any(), any())).thenReturn(iterator);

        assertThatThrownBy(() -> processor.process(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid duplicate value");

        // Ensure we did not write to the store after detecting inconsistency
        verify(stateStore, never()).put(any(), any(), anyLong());
    }
}
