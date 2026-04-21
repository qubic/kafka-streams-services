package org.qubic.tickdata.dedup.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TickDataJsonTest {

    private final ObjectMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Test
    void serialize_toJson_containsAllFields() throws Exception {
        TickData tickData = TickData.builder()
                .computorIndex(257)
                .epoch(209L)
                .tickNumber(49485485L)
                .timestamp(1776261003000L)
                .timeLock("tYwMaI2qus4TEuHjmr4boxUEw5UjSBQPk9Zo0K32Jow=")
                .transactionHashes(List.of("hash1", "hash2"))
                .signature("sig")
                .build();

        String json = mapper.writeValueAsString(tickData);
        JsonNode root = mapper.readTree(json);

        assertThat(root.get("computorIndex").asInt()).isEqualTo(257);
        assertThat(root.get("epoch").asLong()).isEqualTo(209L);
        assertThat(root.get("tickNumber").asLong()).isEqualTo(49485485L);
        assertThat(root.get("timestamp").asLong()).isEqualTo(1776261003000L);
        assertThat(root.get("timeLock").asText()).isEqualTo("tYwMaI2qus4TEuHjmr4boxUEw5UjSBQPk9Zo0K32Jow=");
        assertThat(root.get("transactionHashes").isArray()).isTrue();
        assertThat(root.get("transactionHashes").size()).isEqualTo(2);
        assertThat(root.get("signature").asText()).isEqualTo("sig");
    }

    @Test
    void deserialize_fromJson_populatesModel() throws Exception {
        String json = """
                {
                  "computorIndex": 257,
                  "epoch": 209,
                  "tickNumber": 49485485,
                  "timestamp": 1776261003000,
                  "timeLock": "tYwMaI2qus4TEuHjmr4boxUEw5UjSBQPk9Zo0K32Jow=",
                  "transactionHashes": ["fjemxucngcgcxfiktkllxmfqrxggumhfftvpjsuhmdvmyunazxqluiddpkci", "pbohyzdshhsdjburdxpzrttpahtfnuhhtwyyjxuzhbzxcjpvvwpuwbobvtcn"],
                  "signature": "l50Wx8Ayci0hjJqz1hFiK02f/9IZrY2N3yss7q0VfTRTG2tfAGrBEFRyt62GJV7Bl10re1Id2rhDAUJCm3UMAA=="
                }
                """;

        TickData parsed = mapper.readValue(json, TickData.class);

        assertThat(parsed.getComputorIndex()).isEqualTo(257);
        assertThat(parsed.getEpoch()).isEqualTo(209L);
        assertThat(parsed.getTickNumber()).isEqualTo(49485485L);
        assertThat(parsed.getTimestamp()).isEqualTo(1776261003000L);
        assertThat(parsed.getTimeLock()).isEqualTo("tYwMaI2qus4TEuHjmr4boxUEw5UjSBQPk9Zo0K32Jow=");
        assertThat(parsed.getTransactionHashes()).hasSize(2);
        assertThat(parsed.getSignature()).isEqualTo("l50Wx8Ayci0hjJqz1hFiK02f/9IZrY2N3yss7q0VfTRTG2tfAGrBEFRyt62GJV7Bl10re1Id2rhDAUJCm3UMAA==");
    }

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                  "tickNumber": 49485485,
                  "unknown": "should throw"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, TickData.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("Unrecognized field \"unknown\"");
    }
}
