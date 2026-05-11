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
import org.qubic.transactions.dedup.model.Transaction;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class TransactionDeduplicationProcessorTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final Duration retention = Duration.ofMinutes(5);

    private final ProcessorContext<String, Transaction> context = mock();
    private final WindowStore<String, String> stateStore = mock();
    private final TransactionDeduplicationProcessor processor = new TransactionDeduplicationProcessor("test-store", retention, metrics);

    @BeforeEach
    void setUp() {
        when(context.getStateStore(anyString())).thenReturn(stateStore);
        processor.init(context);
    }

    @Test
    void process_givenUnique_thenForward() {
        Transaction transaction = Transaction.builder()
                .hash("hash")
                .signature("sig")
                .tickNumber(12345)
                .build();
        Record<String, Transaction> record = new Record<>("key", transaction, 1000L);

        final WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(false);
        when(stateStore.fetch(eq("12345:hash"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context).forward(record);
        verify(stateStore).put(eq("12345:hash"), eq("sig"), anyLong());
        assertThat(metrics.get("dedup.messages.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void process_givenDuplicate_thenDoNotForward() {
        Transaction transaction = Transaction.builder()
                .hash("hash")
                .signature("sig")
                .tickNumber(12345)
                .build();
        Record<String, Transaction> record = new Record<>("key", transaction, 1000L);

        final WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(true, false);
        when(stateStore.fetch(eq("12345:hash"), any(), any())).thenReturn(iterator);

        processor.process(record);

        verify(context, never()).forward(any());
        verify(stateStore, never()).put(anyString(), anyString(), anyLong()); // don't update with duplicate
        assertThat(metrics.get("dedup.messages.duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    void process_givenDuplicateWithDifferentValue_thenThrows() {
        Transaction transaction = Transaction.builder()
                .hash("hash")
                .signature("sig")
                .tickNumber(12345)
                .build();
        Record<String, Transaction> record = new Record<>("key", transaction, 1000L);

        final WindowStoreIterator<String> iterator = mock();
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(666L, "sig-unknown")); // wrong value
        when(stateStore.fetch(eq("12345:hash"), any(), any())).thenReturn(iterator);

        assertThatThrownBy(() -> processor.process(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid duplicate value");

        // Ensure we did not write to the store after detecting inconsistency
        verify(stateStore, never()).put(any(), any(), anyLong());
    }
}
