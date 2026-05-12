package org.qubic.transactions.dedup.model;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionJsonTest {

    private final ObjectMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Test
    void serialize_toJson_containsAllFields() {
        Transaction tx = Transaction.builder()
                .hash("tx-hash")
                .source("source-addr")
                .destination("dest-addr")
                .amount(1000L)
                .tickNumber(49189280L)
                .inputType(1)
                .inputSize(10)
                .inputData("data")
                .signature("sig")
                .timestamp(1775990260000L)
                .moneyFlew(true)
                .build();

        String json = mapper.writeValueAsString(tx);
        JsonNode root = mapper.readTree(json);

        assertThat(root.get("hash").asString()).isEqualTo("tx-hash");
        assertThat(root.get("source").asString()).isEqualTo("source-addr");
        assertThat(root.get("destination").asString()).isEqualTo("dest-addr");
        assertThat(root.get("amount").asLong()).isEqualTo(1000L);
        assertThat(root.get("tickNumber").asLong()).isEqualTo(49189280L);
        assertThat(root.get("inputType").asInt()).isEqualTo(1);
        assertThat(root.get("inputSize").asInt()).isEqualTo(10);
        assertThat(root.get("inputData").asString()).isEqualTo("data");
        assertThat(root.get("signature").asString()).isEqualTo("sig");
        assertThat(root.get("timestamp").asLong()).isEqualTo(1775990260000L);
        assertThat(root.get("moneyFlew").asBoolean()).isTrue();
    }

    @Test
    void deserialize_fromJson_populatesModel() {
        String json = """
                {
                  "hash": "tx-hash",
                  "source": "source-addr",
                  "destination": "dest-addr",
                  "amount": 1000,
                  "tickNumber": 49189280,
                  "inputType": 1,
                  "inputSize": 10,
                  "inputData": "data",
                  "signature": "sig",
                  "timestamp": 1775990260000,
                  "moneyFlew": true
                }
                """;

        Transaction parsed = mapper.readValue(json, Transaction.class);

        assertThat(parsed.getHash()).isEqualTo("tx-hash");
        assertThat(parsed.getAmount()).isEqualTo(1000L);
        assertThat(parsed.isMoneyFlew()).isTrue();
    }

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                  "hash": "tx-hash",
                  "amount": 1,
                  "tickNumber": 2,
                  "inputType": 3,
                  "inputSize": 4,
                  "timestamp": 5,
                  "moneyFlew": false,
                  "unknown": "should throw"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, Transaction.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }
}
