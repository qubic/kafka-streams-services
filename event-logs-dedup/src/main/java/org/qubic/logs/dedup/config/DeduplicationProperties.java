package org.qubic.logs.dedup.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "dedup")
public class DeduplicationProperties {

    private String inputTopic;
    private String outputTopic;
    private Duration retentionDuration;
    private String storeName;
    private boolean changeLogEnabled;

}
