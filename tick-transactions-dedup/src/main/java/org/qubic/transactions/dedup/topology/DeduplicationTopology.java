package org.qubic.transactions.dedup.topology;

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
import org.qubic.transactions.dedup.config.DeduplicationProperties;
import org.qubic.transactions.dedup.model.Transaction;
import org.qubic.transactions.dedup.processor.TransactionDeduplicationProcessorSupplier;
import org.qubic.transactions.dedup.serde.TransactionSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Slf4j
@Configuration
public class DeduplicationTopology {

    private final DeduplicationProperties properties;
    private final TransactionSerde transactionSerde;
    private final MeterRegistry meterRegistry;

    public DeduplicationTopology(DeduplicationProperties properties, TransactionSerde transactionSerde, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.transactionSerde = transactionSerde;
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public KStream<String, Transaction> kStream(StreamsBuilder streamsBuilder) {
        log.info("Building Kafka Streams topology for tick transactions...");
        log.info("Input topic: [{}]", properties.getInputTopic());
        log.info("Output topic: [{}]", properties.getOutputTopic());
        log.info("Retention duration: [{}]", properties.getRetentionDuration());

        // window store is not correct for this use case, but it solves the cleanup after retention
        StoreBuilder<WindowStore<String, String>> dedupStoreBuilder =
                Stores.windowStoreBuilder(
                                Stores.persistentWindowStore(
                                        properties.getStoreName(),
                                        properties.getRetentionDuration(),
                                        properties.getWindowSize(),
                                        false
                                ),
                                Serdes.String(),
                                Serdes.String()
                        );

        if (properties.isCachingEnabled()) {
            log.info("Store caching is enabled.");
            dedupStoreBuilder.withCachingEnabled();
        } else {
            log.info("Store caching is disabled.");
            dedupStoreBuilder.withCachingDisabled();
        }

        if (properties.isChangeLogEnabled()) {
            log.info("Store changelog is enabled.");
            dedupStoreBuilder.withLoggingEnabled(new HashMap<>());
        } else {
            log.warn("Store changelog is disabled.");
            dedupStoreBuilder.withLoggingDisabled();
        }

        streamsBuilder.addStateStore(dedupStoreBuilder);

        KStream<String, Transaction> input = streamsBuilder.stream(properties.getInputTopic(), 
                Consumed.with(Serdes.String(), transactionSerde));

        ProcessorSupplier<String, Transaction, String, Transaction> processorSupplier = 
                new TransactionDeduplicationProcessorSupplier(properties.getStoreName(), properties.getRetentionDuration(), meterRegistry);

        KStream<String, Transaction> deduplicated = input.process(processorSupplier, properties.getStoreName());

        deduplicated.to(properties.getOutputTopic(), Produced.with(Serdes.String(), transactionSerde));

        log.info("Topology built successfully.");
        return deduplicated;
    }
}
