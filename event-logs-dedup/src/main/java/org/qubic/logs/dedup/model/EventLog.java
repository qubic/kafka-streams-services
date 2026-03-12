package org.qubic.logs.dedup.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL) // omit null values
public class EventLog {

    private long epoch; // required

    private long tickNumber; // required

    private long index; // required

    private int type; // required

    private long logId; // required

    private long timestamp; // required

    private String transactionHash;

    private Boolean lastLogForTick; // empty if false

    // we do not assume that the body is always present (unclear if true)
    private String logDigest; // hash of the event body
    private Long bodySize;
    private Map<String, Object> body;

}