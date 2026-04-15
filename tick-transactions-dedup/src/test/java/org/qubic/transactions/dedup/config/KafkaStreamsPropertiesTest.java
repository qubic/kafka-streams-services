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
        "streams.application-id=test-app"
})
public class KafkaStreamsPropertiesTest {

    @Autowired
    private KafkaStreamsProperties properties;

    @Test
    void shouldPopulateReplicationFactor() {
        assertThat(properties.getReplicationFactor()).isEqualTo(3);
        
        Map<String, Object> props = properties.asProperties();
        assertThat(props).containsEntry(REPLICATION_FACTOR_CONFIG, 3);
        assertThat(props).containsEntry("application.id", "test-app");
    }

    @EnableConfigurationProperties(KafkaStreamsProperties.class)
    public static class TestConfig {
    }
}
