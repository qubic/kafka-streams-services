package org.qubic.logs.dedup.model;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;

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
                "tick":3,
                "index":4,
                "logId":5,
                "timestamp":123456789,
                "body":{"key1":"value1","num":42},
                "unknown":"should be ignored"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, EventLog.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }

}
