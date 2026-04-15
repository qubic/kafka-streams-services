package org.qubic.tickdata.dedup.processor;

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
import org.qubic.tickdata.dedup.model.TickData;
import org.qubic.tickdata.dedup.serde.TickDataSerde;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("resource")
class TickDataDeduplicationProcessorTopologyTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final String storeName = "test-store";
    private final TickDataSerde tickDataSerde = new TickDataSerde(new ObjectMapper());
    private final Duration retention = Duration.ofMinutes(5);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, TickData> inputTopic;
    private TestOutputTopic<String, TickData> outputTopic;
    private WindowStore<String, String> stateStore;

    @BeforeEach
    void setUp() {
        Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), tickDataSerde.deserializer(), "input-topic");
        topology.addProcessor("processor", () -> new TickDataDeduplicationProcessor(storeName, retention, metrics), "source");
        topology.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.inMemoryWindowStore(storeName,
                                retention,
                                retention,
                                false),
                        Serdes.String(),
                        Serdes.String()
                ),
                "processor"
        );
        topology.addSink("sink", "output-topic", Serdes.String().serializer(), tickDataSerde.serializer(), "processor");

        Properties props = new Properties();
        props.setProperty("application.id", "test-app");
        props.setProperty("bootstrap.servers", "dummy:1234");

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("input-topic", Serdes.String().serializer(), tickDataSerde.serializer());
        outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(), tickDataSerde.deserializer());
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
        TickData event = TickData.builder()
                .epoch(209L)
                .tickNumber(49485485L)
                .signature("sig1")
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, timestamp);

        assertThat(outputTopic.readValuesToList()).containsExactly(event);

        // Verify state store
        WindowStoreIterator<String> iterator = stateStore.fetch("49485485", Instant.EPOCH, Instant.now());
        assertThat(iterator).hasNext();
        assertThat(iterator.next().value).isEqualTo("sig1");
    }

    @Test
    void shouldNotForwardDuplicateRecord() {
        TickData event = TickData.builder()
                .epoch(209L)
                .tickNumber(49485485L)
                .signature("sig1")
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, timestamp);
        inputTopic.pipeInput("key", event, timestamp.plusMillis(50)); // duplicate within retention
        
        assertThat(outputTopic.readValuesToList()).hasSize(1);

        // Verify metrics
        assertThat(metrics.get("dedup.tickdata.processed").counter().count()).isEqualTo(2.0);
        assertThat(metrics.get("dedup.tickdata.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.tickdata.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void shouldForwardDuplicateOutsideRetentionPeriod() {
        TickData event = TickData.builder()
                .epoch(209L)
                .tickNumber(49485485L)
                .signature("sig1")
                .build();

        inputTopic.pipeInput("key", event);
        inputTopic.advanceTime(Duration.ofSeconds(1));
        inputTopic.pipeInput("key", event); // duplicate within retention
        
        inputTopic.advanceTime(retention.plusMinutes(1)); // outside retention
        inputTopic.pipeInput("key", event); // should be forwarded
        
        assertThat(outputTopic.readValuesToList()).hasSize(2);
    }
}
