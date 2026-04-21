package org.qubic.transactions.dedup.config;

import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
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
    private String topologyOptimization;
    private Integer replicationFactor;
    private Integer maxRequestSizeInMb;

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
        if (replicationFactor != null) props.put(REPLICATION_FACTOR_CONFIG, replicationFactor);
        if (maxRequestSizeInMb != null) props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                maxRequestSizeInMb * 1024 * 1024);
        return props;
    }
}
