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

    @JsonProperty("body")
    private Map<String, Object> body;

    @JsonProperty("bodySize")
    private int bodySize;

    @JsonProperty("epoch")
    private int epoch;

    @JsonProperty("logDigest")
    private String logDigest;

    @JsonProperty("logId")
    private long logId;

    @JsonProperty("logTypename")
    private String logTypename;

    @JsonProperty("ok")
    private boolean ok;

    @JsonProperty("tick")
    private long tick;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("txHash")
    private String txHash;

    @JsonProperty("type")
    private int type;
}