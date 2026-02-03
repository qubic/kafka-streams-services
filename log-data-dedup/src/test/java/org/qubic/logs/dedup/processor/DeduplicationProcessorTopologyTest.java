package org.qubic.logs.dedup.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;
import org.qubic.logs.dedup.serde.EventLogSerde;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("resource")
class DeduplicationProcessorTopologyTest {

    private final MeterRegistry metrics = new SimpleMeterRegistry();
    private final String storeName = "test-store";
    private final EventLogSerde eventLogSerde = new EventLogSerde(new ObjectMapper());
    private final Duration retention = Duration.ofMinutes(5);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, EventLog> inputTopic;
    private TestOutputTopic<String, EventLog> outputTopic;
    private WindowStore<String, Long> stateStore;

    @BeforeEach
    void setUp() {
        Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), eventLogSerde.deserializer(), "input-topic");
        topology.addProcessor("processor", () -> new DeduplicationProcessor(storeName, retention, metrics), "source");
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
        topology.addSink("sink", "output-topic", Serdes.String().serializer(), eventLogSerde.serializer(), "processor");

        Properties props = new Properties();
        props.setProperty("application.id", "test-app");
        props.setProperty("bootstrap.servers", "dummy:1234");

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("input-topic", Serdes.String().serializer(), eventLogSerde.serializer());
        outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(), eventLogSerde.deserializer());
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
        EventLog event = EventLog.builder()
                .tick(42)
                .index(101)
                .logDigest("digest-1")
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, timestamp);

        assertThat(outputTopic.readValuesToList()).containsExactly(event);

        // Verify state store
        String dedupKey = event.getTick() + ":" + event.getIndex();
        assertThat(stateStore.fetch(dedupKey, Instant.EPOCH, Instant.now())).hasNext();
    }

    @Test
    void shouldNotForwardDuplicateRecord() {
        EventLog event = EventLog.builder()
                .tick(42)
                .index(101)
                .logDigest("digest-1")
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, Instant.now());
        inputTopic.pipeInput("key", event, timestamp.plusMillis(50)); // duplicate within retention
        assertThat(outputTopic.readValuesToList()).hasSize(1);

        // Verify metrics
        assertThat(metrics.get("dedup.events.processed").counter().count()).isEqualTo(2.0);
        assertThat(metrics.get("dedup.events.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.events.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void shouldForwardOneDuplicateOutsideRetentionPeriod() {
        EventLog event1 = EventLog.builder()
                .tick(42)
                .index(101)
                .logDigest("1")
                .build();

        EventLog event2 = EventLog.builder()
                .tick(42)
                .index(101)
                .logDigest("2")
                .build();

        EventLog event3 = EventLog.builder()
                .tick(42)
                .index(101)
                .logDigest("3")
                .build();

        inputTopic.pipeInput("key", event1);
        inputTopic.advanceTime(Duration.ofSeconds(1));
        inputTopic.pipeInput("key", event2); // duplicate within retention
        inputTopic.advanceTime(retention.plusMinutes(1)); // we truncate to minute per default
        inputTopic.pipeInput("key", event3); // duplicate outside retention
        List<EventLog> forwardedLogs = outputTopic.readValuesToList();
        assertThat(forwardedLogs).hasSize(2);
        assertThat(forwardedLogs).containsExactly(event1, event3);
        // Verify metrics
        assertThat(metrics.get("dedup.events.processed").counter().count()).isEqualTo(3.0);
        assertThat(metrics.get("dedup.events.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.events.unique").counter().count()).isEqualTo(2.0);
    }

}