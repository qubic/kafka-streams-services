package org.qubic.tickdata.dedup.serde;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.qubic.tickdata.dedup.model.TickData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TickDataSerdeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TickDataSerde serde = new TickDataSerde(objectMapper);

    @Test
    void serializeAndDeserialize_roundTrip_ok() {
        TickData original = TickData.builder()
                .computorIndex(257)
                .epoch(209L)
                .tickNumber(49485485L)
                .build();

        byte[] bytes = serde.serializer().serialize("topic", original);
        assertThat(bytes).isNotNull();
        TickData restored = serde.deserializer().deserialize("topic", bytes);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void serialize_null_returnsNull() {
        byte[] bytes = serde.serializer().serialize("topic", null);
        assertThat(bytes).isNull();
    }

    @Test
    void deserialize_null_returnsNull() {
        TickData result = serde.deserializer().deserialize("topic", null);
        assertThat(result).isNull();
    }

    @Test
    void deserialize_invalidBytes_throwsSerializationException() {
        byte[] invalid = "not-json".getBytes();
        assertThatThrownBy(() -> serde.deserializer().deserialize("topic", invalid))
                .isInstanceOf(SerializationException.class);
    }
}
