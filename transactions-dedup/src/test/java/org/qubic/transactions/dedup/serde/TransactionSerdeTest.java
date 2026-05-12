package org.qubic.transactions.dedup.serde;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.Transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionSerdeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TransactionSerde serde = new TransactionSerde(objectMapper);

    @Test
    void serializeAndDeserialize_roundTrip_ok() {
        Transaction original = Transaction.builder()
                .hash("hash1")
                .tickNumber(49189280L)
                .build();

        byte[] bytes = serde.serializer().serialize("topic", original);
        assertThat(bytes).isNotNull();
        Transaction restored = serde.deserializer().deserialize("topic", bytes);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void serialize_null_returnsNull() {
        byte[] bytes = serde.serializer().serialize("topic", null);
        assertThat(bytes).isNull();
    }

    @Test
    void deserialize_null_returnsNull() {
        Transaction result = serde.deserializer().deserialize("topic", null);
        assertThat(result).isNull();
    }

    @Test
    void deserialize_invalidBytes_throwsSerializationException() {
        byte[] invalid = "not-json".getBytes();
        assertThatThrownBy(() -> serde.deserializer().deserialize("topic", invalid))
                .isInstanceOf(SerializationException.class);
    }
}
