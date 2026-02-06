package org.qubic.logs.dedup.model;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class EventLogJsonIT {

    // Use spring bean to make sure it's properly configured
    @Autowired
    private ObjectMapper mapper;

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                "type":1,
                "epoch":2,
                "tickNumber":3,
                "index":4,
                "logId":5,
                "timestamp":123456789,
                "bodySize":6,
                "emittingContractIndex":7,
                "body":{"key1":"value1","num":42},
                "unknown":"should not be ignored"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, EventLog.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }

    @Test
    void deserialize_fromJson_succeed() {
        String json = """
                {
                    "index":19,
                    "emittingContractIndex":0,
                    "type":0,
                    "tickNumber":43141304,
                    "epoch":198,
                    "logDigest":"697847e48b5d36ea",
                    "logId":614864,
                    "bodySize":72,
                    "emittingContractIndex":7,
                    "timestamp":1769660709,
                    "transactionHash":"pvoswpdzrcqasbnknyoeysbgococnivfrqtzhwhvwcvupdubmurwabtfltxo",
                    "body":{"amount":9,"destination":"XHUVLMVIXQXCFGCDULCZGCIPVWHBBECNIZBWFBWQFGTQVXAUALSQYHIDSING","source":"NPGVYDYUFNOQMBYEZFBIQBIBPLHBJHRCOOKKPFIYPEYTSUBOTHHNMPDDELJH"}}
                """;

        assertThatNoException().isThrownBy(() -> mapper.readValue(json, EventLog.class));
    }

}
