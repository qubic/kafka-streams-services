package org.qubic.transactions.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.TickTransactions;
import org.qubic.transactions.dedup.model.Transaction;
import org.qubic.transactions.dedup.serde.TickTransactionsSerde;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("resource")
class TickTransactionsDeduplicationProcessorTopologyTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final String storeName = "test-store";
    private final TickTransactionsSerde tickTransactionsSerde = new TickTransactionsSerde(new ObjectMapper());
    private final Duration retention = Duration.ofMinutes(5);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, TickTransactions> inputTopic;
    private TestOutputTopic<String, TickTransactions> outputTopic;
    private WindowStore<String, Long> stateStore;

    @BeforeEach
    void setUp() {
        Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), tickTransactionsSerde.deserializer(), "input-topic");
        topology.addProcessor("processor", () -> new TickTransactionsDeduplicationProcessor(storeName, retention, metrics), "source");
        topology.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.inMemoryWindowStore(storeName,
                                retention,
                                retention,
                                false),
                        Serdes.String(),
                        Serdes.Long()
                ),
                "processor"
        );
        topology.addSink("sink", "output-topic", Serdes.String().serializer(), tickTransactionsSerde.serializer(), "processor");

        Properties props = new Properties();
        props.setProperty("application.id", "test-app");
        props.setProperty("bootstrap.servers", "dummy:1234");

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("input-topic", Serdes.String().serializer(), tickTransactionsSerde.serializer());
        outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(), tickTransactionsSerde.deserializer());
        stateStore = testDriver.getWindowStore(storeName);
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void shouldForwardNewRecord() {
        TickTransactions event = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .transactions(List.of(new Transaction()))
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, timestamp);

        assertThat(outputTopic.readValuesToList()).containsExactly(event);

        // Verify state store
        WindowStoreIterator<Long> iterator = stateStore.fetch("49189280", Instant.EPOCH, Instant.now());
        assertThat(iterator).hasNext();
        assertThat(iterator.next().value).isEqualTo(1L);
    }

    @Test
    void shouldNotForwardDuplicateRecord() {
        TickTransactions event = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, timestamp);
        inputTopic.pipeInput("key", event, timestamp.plusMillis(50)); // duplicate within retention
        
        assertThat(outputTopic.readValuesToList()).hasSize(1);

        // Verify metrics
        assertThat(metrics.get("dedup.messages.processed").counter().count()).isEqualTo(2.0);
        assertThat(metrics.get("dedup.messages.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.messages.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void shouldForwardDuplicateOutsideRetentionPeriod() {
        TickTransactions event = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .build();

        inputTopic.pipeInput("key", event);
        inputTopic.advanceTime(Duration.ofSeconds(1));
        inputTopic.pipeInput("key", event); // duplicate within retention
        
        inputTopic.advanceTime(retention.plusMinutes(1)); // outside retention
        inputTopic.pipeInput("key", event); // should be forwarded
        
        assertThat(outputTopic.readValuesToList()).hasSize(2);
    }
}
