package org.qubic.transactions.dedup.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = KafkaStreamsPropertiesTest.TestConfig.class)
@TestPropertySource(properties = {
        "streams.replication-factor=3",
        "streams.application-id=test-app",
        "streams.max-request-size-in-mb=5"
})
public class KafkaStreamsPropertiesTest {

    @Autowired
    private KafkaStreamsProperties properties;

    @Test
    void shouldPopulateProperties() {
        assertThat(properties.getReplicationFactor()).isEqualTo(3);
        assertThat(properties.getMaxRequestSizeInMb()).isEqualTo(5);
        
        Map<String, Object> props = properties.asProperties();
        assertThat(props).containsEntry(REPLICATION_FACTOR_CONFIG, 3);
        assertThat(props).containsEntry("application.id", "test-app");
        assertThat(props).containsEntry("producer.max.request.size", 5 * 1024 * 1024);
    }

    @EnableConfigurationProperties(KafkaStreamsProperties.class)
    public static class TestConfig {
    }
}
