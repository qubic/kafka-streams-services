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
import org.apache.kafka.streams.state.WindowStoreIterator;
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
    private final Duration retention = Duration.ofMinutes(100);
    private final Duration window = Duration.ofMinutes(10);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, EventLog> inputTopic;
    private TestOutputTopic<String, EventLog> outputTopic;
    private WindowStore<String, String> stateStore;

    @BeforeEach
    void setUp() {
        Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), eventLogSerde.deserializer(), "input-topic");
        topology.addProcessor("processor", () -> new DeduplicationProcessor(storeName, null, retention, metrics), "source");
        topology.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.inMemoryWindowStore(storeName,
                                retention,
                                window,
                                false),
                        Serdes.String(),
                        Serdes.String()
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
                .epoch(123)
                .tickNumber(1)
                .index(2)
                .type(3)
                .logId(4)
                .logDigest("digest-1")
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, timestamp);

        assertThat(outputTopic.readValuesToList()).containsExactly(event);

        // Verify state store
        String dedupKey = event.getEpoch() + ":" + event.getLogId();
        WindowStoreIterator<String> iterator = stateStore.fetch(dedupKey, Instant.EPOCH, Instant.now());
        assertThat(iterator).hasNext();
        assertThat(iterator.next().value).isEqualTo(event.getTickNumber() + ":" + event.getIndex() + ":" + event.getType() + ":" + event.getLogDigest());

    }

    @Test
    void shouldNotForwardDuplicateRecord() {
        EventLog event = EventLog.builder()
                .tickNumber(42)
                .index(101)
                .logDigest("digest-1")
                .build();

        Instant timestamp = Instant.now();
        inputTopic.pipeInput("key", event, Instant.now());
        inputTopic.pipeInput("key", event, timestamp.plusMillis(50)); // duplicate within retention
        assertThat(outputTopic.readValuesToList()).hasSize(1);

        // Verify metrics
        assertThat(metrics.get("dedup.messages.processed").counter().count()).isEqualTo(2.0);
        assertThat(metrics.get("dedup.messages.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.messages.unique").counter().count()).isEqualTo(1.0);
    }

    @Test
    void shouldForwardOneDuplicateOutsideRetentionPeriod() {
        EventLog event1 = EventLog.builder()
                .index(101)
                .build();

        EventLog event2 = EventLog.builder()
                .index(101)
                .build();

        EventLog event3 = EventLog.builder()
                .index(101)
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
        assertThat(metrics.get("dedup.messages.processed").counter().count()).isEqualTo(3.0);
        assertThat(metrics.get("dedup.messages.duplicate").counter().count()).isEqualTo(1.0);
        assertThat(metrics.get("dedup.messages.unique").counter().count()).isEqualTo(2.0);
    }

    @Test
    void shouldFindDuplicatesAtVariousPointsOfRetentionPeriod() {
        // Use a fixed base time truncated to minutes for predictability
        Instant baseTime = Instant.parse("2026-04-22T10:00:00Z");

        // 1. Store records at different points in time
        EventLog event1 = EventLog.builder().epoch(1).logId(1).build();
        inputTopic.pipeInput("key1", event1, baseTime);

        EventLog event2 = EventLog.builder().epoch(1).logId(2).build();
        inputTopic.pipeInput("key2", event2, baseTime.plus(retention.dividedBy(2)));

        EventLog event3 = EventLog.builder().epoch(1).logId(3).build();
        inputTopic.pipeInput("key3", event3, baseTime.plus(retention).minusSeconds(1));

        assertThat(outputTopic.readValuesToList()).hasSize(3);

        // 2. At a later time, verify all are found as duplicates
        // We use baseTime + retention as the current time.
        // Retention window for baseTime + retention is [baseTime, MAX]
        Instant now = baseTime.plus(retention);

        // Key1 (stored at baseTime) - should be found (exactly at the beginning of the window)
        inputTopic.pipeInput("key1", event1, now);
        // Key2 (stored at baseTime + retention/2) - should be found (in the middle of the window)
        inputTopic.pipeInput("key2", event2, now);
        // Key3 (stored at baseTime + retention - 1s) - should be found (near the end of the window)
        inputTopic.pipeInput("key3", event3, now);

        assertThat(outputTopic.readValuesToList()).isEmpty();

        // Verify total metrics
        assertThat(metrics.get("dedup.messages.processed").counter().count()).isEqualTo(6.0);
        assertThat(metrics.get("dedup.messages.duplicate").counter().count()).isEqualTo(3.0);
        assertThat(metrics.get("dedup.messages.unique").counter().count()).isEqualTo(3.0);
    }

}