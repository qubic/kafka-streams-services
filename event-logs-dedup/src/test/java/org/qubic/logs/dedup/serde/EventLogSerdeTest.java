package org.qubic.logs.dedup.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventLogSerdeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final EventLogSerde serde = new EventLogSerde(objectMapper);

    @Test
    void serializeAndDeserialize_roundTrip_ok() {
        Map<String, Object> body = new HashMap<>();
        body.put("key1", "value1");
        body.put("num", 42);

        EventLog original = EventLog.builder()
                .type(1)
                .epoch(2)
                .tickNumber(3)
                .index(4)
                .logId(5)
                .bodySize(6L)
                .logDigest("digest")
                .transactionHash("transactionHash")
                .timestamp(1234567890)
                .body(body)
                .lastLogForTick(true)
                .build();

        byte[] bytes = serde.serializer().serialize("topic", original);
        assertThat(bytes).isNotNull();
        EventLog restored = serde.deserializer().deserialize("topic", bytes);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void serializeAndDeserialize_roundTripOnlyOptional_ok() {
        EventLog original = EventLog.builder()
                .type(1)
                .epoch(2)
                .tickNumber(3)
                .index(4)
                .logId(5)
                .timestamp(1234567890)
                .build();

        byte[] bytes = serde.serializer().serialize("topic", original);
        assertThat(bytes).isNotNull();
        EventLog restored = serde.deserializer().deserialize("topic", bytes);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void serialize_null_returnsNull() {
        byte[] bytes = serde.serializer().serialize("topic", null);
        assertThat(bytes).isNull();
    }

    @Test
    void deserialize_null_returnsNull() {
        EventLog result = serde.deserializer().deserialize("topic", null);
        assertThat(result).isNull();
    }

    @Test
    void deserialize_invalidBytes_throwsSerializationException() {
        byte[] invalid = "not-json".getBytes();
        assertThatThrownBy(() -> serde.deserializer().deserialize("topic", invalid))
                .isInstanceOf(SerializationException.class);
    }
}
