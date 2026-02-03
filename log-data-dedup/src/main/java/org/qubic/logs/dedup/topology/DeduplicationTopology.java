package org.qubic.logs.dedup.topology;

import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.qubic.logs.dedup.model.EventLog;
import org.qubic.logs.dedup.processor.DeduplicationProcessorSupplier;
import org.qubic.logs.dedup.serde.EventLogSerde;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.HashMap;

@Slf4j
@Configuration
public class DeduplicationTopology {

    @Value("${dedup.input-topic}")
    private String inputTopic;

    @Value("${dedup.output-topic}")
    private String outputTopic;

    @Value("${dedup.retention-days}")
    private long retentionDays;

    @Value("${dedup.store-name}")
    private String storeName;

    private final EventLogSerde eventLogSerde;

    private final MeterRegistry meterRegistry;

    public DeduplicationTopology(EventLogSerde eventLogSerde, MeterRegistry meterRegistry) {
        this.eventLogSerde = eventLogSerde;
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public KStream<String, EventLog> kStream(StreamsBuilder streamsBuilder) {
        log.info("Building Kafka Streams topology");
        log.info("Input topic: {}", inputTopic);
        log.info("Output topic: {}", outputTopic);
        log.info("Retention: {} days", retentionDays);

        Duration retentionDuration = Duration.ofDays(retentionDays);

        // Create a window store for deduplication with automatic expiry
        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder =
                Stores.windowStoreBuilder(
                                Stores.persistentWindowStore(
                                        storeName,
                                        retentionDuration,
                                        retentionDuration,
                                        false
                                ),
                                Serdes.String(),
                                Serdes.Long()
                        )
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>());  // Changelog for fault tolerance. Map contains optional topic configuration.
        streamsBuilder.addStateStore(dedupStoreBuilder);

        // Input stream
        KStream<String, EventLog> input = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), eventLogSerde));

        // Deduplication transformation
        ProcessorSupplier<String, EventLog, String, EventLog> processorSupplier = new DeduplicationProcessorSupplier(storeName, retentionDuration, meterRegistry);
        KStream<String, EventLog> deduplicated = input.process(processorSupplier, storeName);

        // Output stream (with logDigest as key for consistent partitioning)
        deduplicated.to(outputTopic, Produced.with(Serdes.String(), eventLogSerde));

        log.info("Topology built successfully");
        return deduplicated;
    }
}
