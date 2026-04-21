package org.qubic.transactions.dedup.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = DeduplicationPropertiesTest.TestConfig.class)
@TestPropertySource(properties = {
        "dedup.input-topic=test-input",
        "dedup.output-topic=test-output",
        "dedup.retention-duration=10m",
        "dedup.store-name=test-store",
        "dedup.change-log-enabled=false",
        "dedup.caching-enabled=true"
})
public class DeduplicationPropertiesTest {

    @Autowired
    private DeduplicationProperties properties;

    @Test
    void shouldPopulateProperties() {
        assertThat(properties.getInputTopic()).isEqualTo("test-input");
        assertThat(properties.getOutputTopic()).isEqualTo("test-output");
        assertThat(properties.getRetentionDuration()).isEqualTo(Duration.ofMinutes(10));
        assertThat(properties.getStoreName()).isEqualTo("test-store");
        assertThat(properties.isChangeLogEnabled()).isFalse();
        assertThat(properties.isCachingEnabled()).isTrue();
    }

    @EnableConfigurationProperties(DeduplicationProperties.class)
    public static class TestConfig {
    }
}
