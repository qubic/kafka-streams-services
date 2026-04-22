package org.qubic.logs.dedup.config;

import lombok.Data;
import org.apache.commons.lang3.Range;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;

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
    private Map<Long, List<Range<Long>>> ignoredLogKeys;

}
