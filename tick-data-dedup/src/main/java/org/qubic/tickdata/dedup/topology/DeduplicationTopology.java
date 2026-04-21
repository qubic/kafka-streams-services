package org.qubic.tickdata.dedup.topology;

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
import org.qubic.tickdata.dedup.config.DeduplicationProperties;
import org.qubic.tickdata.dedup.model.TickData;
import org.qubic.tickdata.dedup.processor.TickDataDeduplicationProcessorSupplier;
import org.qubic.tickdata.dedup.serde.TickDataSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Slf4j
@Configuration
public class DeduplicationTopology {

    private final DeduplicationProperties properties;
    private final TickDataSerde tickDataSerde;
    private final MeterRegistry meterRegistry;

    public DeduplicationTopology(DeduplicationProperties properties, TickDataSerde tickDataSerde, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.tickDataSerde = tickDataSerde;
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public KStream<String, TickData> kStream(StreamsBuilder streamsBuilder) {
        log.info("Building Kafka Streams topology for tick data...");
        log.info("Input topic: [{}]", properties.getInputTopic());
        log.info("Output topic: [{}]", properties.getOutputTopic());
        log.info("Retention duration: [{}]", properties.getRetentionDuration());

        StoreBuilder<WindowStore<String, String>> dedupStoreBuilder =
                Stores.windowStoreBuilder(
                                Stores.persistentWindowStore(
                                        properties.getStoreName(),
                                        properties.getRetentionDuration(),
                                        properties.getRetentionDuration(),
                                        false
                                ),
                                Serdes.String(),
                                Serdes.String()
                        );
//                        .withCachingEnabled();

        if (properties.isCachingEnabled()) {
            dedupStoreBuilder.withCachingEnabled();
        } else {
            dedupStoreBuilder.withCachingDisabled();
        }


        if (properties.isChangeLogEnabled()) {
            log.info("Changelog is enabled.");
            dedupStoreBuilder.withLoggingEnabled(new HashMap<>());
        } else {
            log.warn("Changelog is disabled.");
            dedupStoreBuilder.withLoggingDisabled();
        }
        streamsBuilder.addStateStore(dedupStoreBuilder);

        KStream<String, TickData> input = streamsBuilder.stream(properties.getInputTopic(),
                Consumed.with(Serdes.String(), tickDataSerde));

        ProcessorSupplier<String, TickData, String, TickData> processorSupplier =
                new TickDataDeduplicationProcessorSupplier(properties.getStoreName(), properties.getRetentionDuration(), meterRegistry);

        KStream<String, TickData> deduplicated = input.process(processorSupplier, properties.getStoreName());

        deduplicated.to(properties.getOutputTopic(), Produced.with(Serdes.String(), tickDataSerde));

        log.info("Topology built successfully.");
        return deduplicated;
    }
}
