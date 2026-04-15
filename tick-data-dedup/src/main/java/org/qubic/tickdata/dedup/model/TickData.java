package org.qubic.tickdata.dedup.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TickData {

    private int computorIndex;
    private long epoch;
    private long tickNumber;
    private long timestamp;
    private String timeLock;
    private List<String> transactionHashes;
    private String signature;

}
