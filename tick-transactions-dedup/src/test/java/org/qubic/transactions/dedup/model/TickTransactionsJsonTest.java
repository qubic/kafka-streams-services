package org.qubic.transactions.dedup.model;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TickTransactionsJsonTest {

    private final ObjectMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Test
    void serialize_toJson_containsAllFields() throws Exception {
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

        TickTransactions tickTransactions = TickTransactions.builder()
                .epoch(208L)
                .tickNumber(49189280L)
                .transactions(List.of(tx))
                .build();

        String json = mapper.writeValueAsString(tickTransactions);
        JsonNode root = mapper.readTree(json);

        assertThat(root.get("epoch").asLong()).isEqualTo(208L);
        assertThat(root.get("tickNumber").asLong()).isEqualTo(49189280L);
        assertThat(root.get("transactions").isArray()).isTrue();
        assertThat(root.get("transactions").size()).isEqualTo(1);

        JsonNode txNode = root.get("transactions").get(0);
        assertThat(txNode.get("hash").asText()).isEqualTo("tx-hash");
        assertThat(txNode.get("source").asText()).isEqualTo("source-addr");
        assertThat(txNode.get("destination").asText()).isEqualTo("dest-addr");
        assertThat(txNode.get("amount").asLong()).isEqualTo(1000L);
        assertThat(txNode.get("tickNumber").asLong()).isEqualTo(49189280L);
        assertThat(txNode.get("inputType").asInt()).isEqualTo(1);
        assertThat(txNode.get("inputSize").asInt()).isEqualTo(10);
        assertThat(txNode.get("inputData").asText()).isEqualTo("data");
        assertThat(txNode.get("signature").asText()).isEqualTo("sig");
        assertThat(txNode.get("timestamp").asLong()).isEqualTo(1775990260000L);
        assertThat(txNode.get("moneyFlew").asBoolean()).isTrue();
    }

    @Test
    void deserialize_fromJson_populatesModel() throws Exception {
        String json = """
                {
                  "epoch": 208,
                  "tickNumber": 49189280,
                  "transactions": [
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
                  ]
                }
                """;

        TickTransactions parsed = mapper.readValue(json, TickTransactions.class);

        assertThat(parsed.getEpoch()).isEqualTo(208L);
        assertThat(parsed.getTickNumber()).isEqualTo(49189280L);
        assertThat(parsed.getTransactions()).hasSize(1);

        Transaction tx = parsed.getTransactions().get(0);
        assertThat(tx.getHash()).isEqualTo("tx-hash");
        assertThat(tx.getAmount()).isEqualTo(1000L);
        assertThat(tx.isMoneyFlew()).isTrue();
    }

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                  "epoch": 208,
                  "tickNumber": 49189280,
                  "unknown": "should throw"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, TickTransactions.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }
}
