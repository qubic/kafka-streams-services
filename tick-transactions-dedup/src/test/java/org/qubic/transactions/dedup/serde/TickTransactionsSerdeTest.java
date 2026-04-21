package org.qubic.transactions.dedup.serde;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.TickTransactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TickTransactionsSerdeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TickTransactionsSerde serde = new TickTransactionsSerde(objectMapper);

    @Test
    void serializeAndDeserialize_roundTrip_ok() {
        TickTransactions original = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .build();

        byte[] bytes = serde.serializer().serialize("topic", original);
        assertThat(bytes).isNotNull();
        TickTransactions restored = serde.deserializer().deserialize("topic", bytes);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void serialize_null_returnsNull() {
        byte[] bytes = serde.serializer().serialize("topic", null);
        assertThat(bytes).isNull();
    }

    @Test
    void deserialize_null_returnsNull() {
        TickTransactions result = serde.deserializer().deserialize("topic", null);
        assertThat(result).isNull();
    }

    @Test
    void deserialize_invalidBytes_throwsSerializationException() {
        byte[] invalid = "not-json".getBytes();
        assertThatThrownBy(() -> serde.deserializer().deserialize("topic", invalid))
                .isInstanceOf(SerializationException.class);
    }
}
