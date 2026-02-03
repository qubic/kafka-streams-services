package org.qubic.logs.dedup.topology;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.qubic.logs.dedup.config.DeduplicationProperties;
import org.qubic.logs.dedup.model.EventLog;
import org.qubic.logs.dedup.processor.DeduplicationProcessorSupplier;
import org.qubic.logs.dedup.serde.EventLogSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Slf4j
@Configuration
public class DeduplicationTopology {

    private final DeduplicationProperties properties;
    private final EventLogSerde eventLogSerde;
    private final MeterRegistry meterRegistry;

    public DeduplicationTopology(DeduplicationProperties properties, EventLogSerde eventLogSerde, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.eventLogSerde = eventLogSerde;
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public KStream<String, EventLog> kStream(StreamsBuilder streamsBuilder) {
        log.info("Building Kafka Streams topology...");
        log.info("Input topic: [{}]", properties.getInputTopic());
        log.info("Output topic: [{}]", properties.getOutputTopic());
        log.info("Retention duration: [{}]", properties.getRetentionDuration());

        // Create a window store for deduplication with automatic expiry
        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder =
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(
                                properties.getStoreName(),
                                properties.getRetentionDuration(),
                                properties.getRetentionDuration(),
                                false
                        ),
                        Serdes.String(),
                        Serdes.Long()
                )
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>());  // Changelog for fault tolerance. Optional topic configuration.
        streamsBuilder.addStateStore(dedupStoreBuilder);

        // Input stream
        KStream<String, EventLog> input = streamsBuilder.stream(properties.getInputTopic(), Consumed.with(Serdes.String(), eventLogSerde));

        // Deduplication transformation
        ProcessorSupplier<String, EventLog, String, EventLog> processorSupplier =
                new DeduplicationProcessorSupplier(properties.getStoreName(), properties.getRetentionDuration(), meterRegistry);
        KStream<String, EventLog> deduplicated = input.process(processorSupplier, properties.getStoreName());

        // Output stream (with logDigest as key for consistent partitioning)
        deduplicated.to(properties.getOutputTopic(), Produced.with(Serdes.String(), eventLogSerde));

        log.info("Topology built successfully.");
        return deduplicated;
    }
}
