package org.qubic.logs.dedup.model;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventLogJsonTest {

    private final ObjectMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Test
    void serialize_toJson_containsAllFields() {

        Map<String, Object> body = new HashMap<>();
        body.put("key1", "value1");
        body.put("num", 42);

        EventLog log = EventLog.builder()
                .type(1)
                .epoch(2)
                .tick(3L)
                .index(4L)
                .logId(5L)
                .logDigest("digest")
                .txHash("transactionHash")
                .timestamp(123456789L)
                .body(body)
                .build();

        String json = mapper.writeValueAsString(log);
        JsonNode root = mapper.readTree(json);

        assertThat(root.get("type").asInt()).isEqualTo(1);
        assertThat(root.get("epoch").asInt()).isEqualTo(2);
        assertThat(root.get("tick").asLong()).isEqualTo(3L);
        assertThat(root.get("index").asLong()).isEqualTo(4L);
        assertThat(root.get("logId").asLong()).isEqualTo(5L);
        assertThat(root.get("logDigest").asString()).isEqualTo("digest");
        assertThat(root.get("txHash").asString()).isEqualTo("transactionHash");
        assertThat(root.get("timestamp").asLong()).isEqualTo(123456789L);

        JsonNode bodyNode = root.get("body");
        assertThat(bodyNode).isNotNull();
        assertThat(bodyNode.get("key1").asString()).isEqualTo("value1");
        assertThat(bodyNode.get("num").asInt()).isEqualTo(42);
    }

    @Test
    void deserialize_fromJson_populatesModel() {
        String json = """
                {
                "type":1,
                "epoch":2,
                "tick":3,
                "index":4,
                "logId":5,
                "logDigest":"digest",
                "txHash":"transactionHash",
                "timestamp":123456789,
                "body":{"key1":"value1","num":42}
                }
                """;

        EventLog parsed = mapper.readValue(json, EventLog.class);

        assertThat(parsed.getType()).isEqualTo(1);
        assertThat(parsed.getEpoch()).isEqualTo(2);
        assertThat(parsed.getTick()).isEqualTo(3L);
        assertThat(parsed.getIndex()).isEqualTo(4L);
        assertThat(parsed.getLogId()).isEqualTo(5L);
        assertThat(parsed.getLogDigest()).isEqualTo("digest");
        assertThat(parsed.getTxHash()).isEqualTo("transactionHash");
        assertThat(parsed.getTimestamp()).isEqualTo(123456789L);
        assertThat(parsed.getBody()).isNotNull();
        assertThat(parsed.getBody().get("key1")).isEqualTo("value1");
        assertThat(((Number) parsed.getBody().get("num")).intValue()).isEqualTo(42);
    }

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                "type":1,
                "epoch":2,
                "tick":3,
                "index":4,
                "logId":5,
                "logDigest":"digest",
                "txHash":"transactionHash",
                "timestamp":123456789,
                "body":{"key1":"value1","num":42},
                "unknown":"should throw"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, EventLog.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }

}
