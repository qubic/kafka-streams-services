package org.qubic.logs.dedup.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Data
@ConfigurationProperties(prefix = "streams")
public class KafkaStreamsProperties {

    private List<String> bootstrapServers;
    private String applicationId;
    private String stateDir;
    private String processingGuarantee;
    private Long commitIntervalMs;
    private Long cacheMaxBytesBuffering;
    private Integer numStreamThreads;
    private String defaultKeySerde;
    private String defaultValueSerde;
    private String topologyOptimization;

    /**
     * Map properties to a Kafka Streams configuration map.
     */
    public Map<String, Object> asProperties() {
        Map<String, Object> props = new HashMap<>();
        if (bootstrapServers != null && !bootstrapServers.isEmpty()) props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (applicationId != null) props.put(APPLICATION_ID_CONFIG, applicationId);
        if (stateDir != null) props.put(STATE_DIR_CONFIG, stateDir);
        if (processingGuarantee != null) props.put(PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        if (commitIntervalMs != null) props.put(COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        if (cacheMaxBytesBuffering != null) props.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, cacheMaxBytesBuffering);
        if (numStreamThreads != null) props.put(NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        if (topologyOptimization != null) props.put(TOPOLOGY_OPTIMIZATION_CONFIG, topologyOptimization);
        return props;
    }
}
