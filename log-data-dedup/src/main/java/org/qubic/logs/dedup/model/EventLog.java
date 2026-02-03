package org.qubic.logs.dedup.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventLog {

    @JsonProperty("type")
    private int type;

    @JsonProperty("epoch")
    private int epoch;

    @JsonProperty("tick")
    private long tick;

    @JsonProperty("index")
    private long index;

    @JsonProperty("logId")
    private long logId;

    @JsonProperty("logDigest")
    private String logDigest;

    @JsonProperty("txHash")
    private String txHash;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("body")
    private Map<String, Object> body;

    @JsonProperty("bodySize")
    private int bodySize;

}