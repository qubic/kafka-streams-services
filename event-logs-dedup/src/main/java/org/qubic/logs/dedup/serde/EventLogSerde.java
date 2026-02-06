package org.qubic.logs.dedup.serde;

import org.qubic.logs.dedup.model.EventLog;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class EventLogSerde implements Serde<EventLog> {

    private final ObjectMapper objectMapper;

    public EventLogSerde(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Serializer<EventLog> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing EventLog", e);
            }
        };
    }

    @Override
    public Deserializer<EventLog> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, EventLog.class);
            } catch (Exception e) {
                throw new SerializationException("Error deserializing EventLog", e);
            }
        };
    }
}