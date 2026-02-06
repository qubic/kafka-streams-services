package org.qubic.logs.dedup.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class EventLog {

    private long epoch;

    private long tickNumber;

    private long index;

    private int type;

    private long emittingContractIndex;

    private long logId;

    private String logDigest;

    private String transactionHash;

    private long timestamp;

    private long bodySize;

    private Map<String, Object> body;

}