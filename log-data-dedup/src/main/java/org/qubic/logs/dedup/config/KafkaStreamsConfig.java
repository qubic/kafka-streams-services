package org.qubic.logs.dedup.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.qubic.logs.dedup.serde.EventLogSerde;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

@Slf4j
@Configuration
@EnableConfigurationProperties({KafkaStreamsProperties.class, DeduplicationProperties.class})
public class KafkaStreamsConfig {

    private final KafkaStreamsProperties streamsProperties;
    private final ConfigurableApplicationContext applicationContext;

    public KafkaStreamsConfig(KafkaStreamsProperties streamsProperties,
                              ConfigurableApplicationContext applicationContext) {
        this.streamsProperties = streamsProperties;
        this.applicationContext = applicationContext;
    }

    @SuppressWarnings("resource")
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>(streamsProperties.asProperties());
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventLogSerde.class.getName());
        log.info("Kafka Streams configuration: {}", props);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return factoryBean -> {

            factoryBean.setStateListener((newState, oldState) -> {
                log.info("State transition: {} -> {}", oldState, newState);
                if (newState == KafkaStreams.State.ERROR) {
                    log.error("Kafka Streams entered ERROR state");
                    try {
                        log.info("Closing application context");
                        applicationContext.close();
                    } catch (Exception e) {
                        log.warn("Error while closing application context during shutdown", e);
                    }
                }
            });

            // unexpected exception should never continue as we rely on deduplication.
            // shut down all dedup stream applications in case of an unexpected error for error analysis.
            factoryBean.setStreamsUncaughtExceptionHandler(throwable -> {
                log.error("Uncaught exception in Kafka Streams", throwable);
                // The following will transition into the ERROR state (see above).
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });
        };
    }

}
