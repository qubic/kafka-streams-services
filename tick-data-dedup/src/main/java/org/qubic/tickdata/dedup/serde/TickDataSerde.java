package org.qubic.tickdata.dedup.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.qubic.tickdata.dedup.model.TickData;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class TickDataSerde implements Serde<TickData> {

    private final ObjectMapper objectMapper;

    public TickDataSerde(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Serializer<TickData> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing TickData", e);
            }
        };
    }

    @Override
    public Deserializer<TickData> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, TickData.class);
            } catch (Exception e) {
                throw new SerializationException("Error deserializing TickData", e);
            }
        };
    }
}
