package org.qubic.transactions.dedup.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.qubic.transactions.dedup.model.TickTransactions;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class TickTransactionsSerde implements Serde<TickTransactions> {

    private final ObjectMapper objectMapper;

    public TickTransactionsSerde(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Serializer<TickTransactions> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing TickTransactions", e);
            }
        };
    }

    @Override
    public Deserializer<TickTransactions> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, TickTransactions.class);
            } catch (Exception e) {
                throw new SerializationException("Error deserializing TickTransactions", e);
            }
        };
    }
}
