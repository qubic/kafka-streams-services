package org.qubic.transactions.dedup.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.qubic.transactions.dedup.model.Transaction;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class TransactionSerde implements Serde<Transaction> {

    private final ObjectMapper objectMapper;

    public TransactionSerde(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Serializer<Transaction> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing Transaction", e);
            }
        };
    }

    @Override
    public Deserializer<Transaction> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, Transaction.class);
            } catch (Exception e) {
                throw new SerializationException("Error deserializing Transaction", e);
            }
        };
    }
}
