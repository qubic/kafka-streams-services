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
import org.qubic.transactions.dedup.model.TickTransactions;
import org.qubic.transactions.dedup.processor.TickTransactionsDeduplicationProcessorSupplier;
import org.qubic.transactions.dedup.serde.TickTransactionsSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Slf4j
@Configuration
public class DeduplicationTopology {

    private final DeduplicationProperties properties;
    private final TickTransactionsSerde tickTransactionsSerde;
    private final MeterRegistry meterRegistry;

    public DeduplicationTopology(DeduplicationProperties properties, TickTransactionsSerde tickTransactionsSerde, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.tickTransactionsSerde = tickTransactionsSerde;
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public KStream<String, TickTransactions> kStream(StreamsBuilder streamsBuilder) {
        log.info("Building Kafka Streams topology for tick transactions...");
        log.info("Input topic: [{}]", properties.getInputTopic());
        log.info("Output topic: [{}]", properties.getOutputTopic());
        log.info("Retention duration: [{}]", properties.getRetentionDuration());

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
                        .withCachingEnabled();

        if (properties.isChangeLogEnabled()) {
            log.info("Changelog is enabled.");
            dedupStoreBuilder.withLoggingEnabled(new HashMap<>());
        } else {
            log.warn("Changelog is disabled.");
            dedupStoreBuilder.withLoggingDisabled();
        }
        streamsBuilder.addStateStore(dedupStoreBuilder);

        KStream<String, TickTransactions> input = streamsBuilder.stream(properties.getInputTopic(), 
                Consumed.with(Serdes.String(), tickTransactionsSerde));

        ProcessorSupplier<String, TickTransactions, String, TickTransactions> processorSupplier = 
                new TickTransactionsDeduplicationProcessorSupplier(properties.getStoreName(), properties.getRetentionDuration(), meterRegistry);

        KStream<String, TickTransactions> deduplicated = input.process(processorSupplier, properties.getStoreName());

        deduplicated.to(properties.getOutputTopic(), Produced.with(Serdes.String(), tickTransactionsSerde));

        log.info("Topology built successfully.");
        return deduplicated;
    }
}
