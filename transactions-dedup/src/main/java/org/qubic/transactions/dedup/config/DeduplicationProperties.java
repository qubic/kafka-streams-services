package org.qubic.transactions.dedup.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "dedup", ignoreUnknownFields = false)
public class DeduplicationProperties {

    private String inputTopic;
    private String outputTopic;
    private Duration retentionDuration;
    private Duration windowSize;
    private String storeName;
    private boolean changeLogEnabled;
    private boolean cachingEnabled;

}
