package org.qubic.logs.dedup.config;

import org.apache.commons.lang3.Range;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {DeduplicationPropertiesTest.TestConfig.class, CustomRangeConverter.class})
@TestPropertySource(properties = {
        "dedup.input-topic=test-input",
        "dedup.output-topic=test-output",
        "dedup.retention-duration=1h",
        "dedup.window-size=5m",
        "dedup.ignored-log-keys.1=1-3,5-7",
        "dedup.ignored-log-keys.2=2",
})
public class DeduplicationPropertiesTest {

    @Autowired
    private DeduplicationProperties properties;

    @Test
    void shouldPopulateIgnoredLogKeys() {
        assertThat(properties.getIgnoredLogKeys()).isNotNull();
        assertThat(properties.getIgnoredLogKeys()).hasSize(2);
        assertThat(properties.getIgnoredLogKeys())
                .contains(entry(1L, List.of(Range.of(1L, 3L), Range.of(5L, 7L))));
        assertThat(properties.getIgnoredLogKeys())
                .contains(entry(2L, List.of(Range.of(2L, 2L))));
    }

    @Test
    void shouldPopulateOtherProperties() {
        assertThat(properties.getInputTopic()).isEqualTo("test-input");
        assertThat(properties.getOutputTopic()).isEqualTo("test-output");
        assertThat(properties.getRetentionDuration().toHours()).isEqualTo(1);
        assertThat(properties.getWindowSize().toMinutes()).isEqualTo(5);
    }

    @EnableConfigurationProperties(DeduplicationProperties.class)
    public static class TestConfig {
    }
}
