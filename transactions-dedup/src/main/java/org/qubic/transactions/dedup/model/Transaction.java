package org.qubic.transactions.dedup.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Transaction {

    private String hash;
    private String source;
    private String destination;
    private long amount;
    private long tickNumber;
    private int inputType;
    private int inputSize;
    private String inputData;
    private String signature;
    private long timestamp;
    private boolean moneyFlew;

}
