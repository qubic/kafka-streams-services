package org.qubic.tickdata.dedup.model;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class TickDataJsonIT {

    // Use spring bean to make sure it's properly configured
    @Autowired
    private ObjectMapper mapper;

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                "computorIndex":257,
                "epoch":209,
                "tickNumber":49485485,
                "timestamp":1776261003000,
                "timeLock":"tYwMaI2qus4TEuHjmr4boxUEw5UjSBQPk9Zo0K32Jow=",
                "transactionHashes":["fjemxucngcgcxfiktkllxmfqrxggumhfftvpjsuhmdvmyunazxqluiddpkci"],
                "signature":"l50Wx8Ayci0hjJqz1hFiK02f/9IZrY2N3yss7q0VfTRTG2tfAGrBEFRyt62GJV7Bl10re1Id2rhDAUJCm3UMAA==",
                "unknown":"should not be ignored"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, TickData.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }

    @Test
    void deserialize_fromJson_succeed() {
        String json = """
                {"computorIndex":257,"epoch":209,"tickNumber":49485485,"timestamp":1776261003000,"timeLock":"tYwMaI2qus4TEuHjmr4boxUEw5UjSBQPk9Zo0K32Jow=","transactionHashes":["fjemxucngcgcxfiktkllxmfqrxggumhfftvpjsuhmdvmyunazxqluiddpkci","pbohyzdshhsdjburdxpzrttpahtfnuhhtwyyjxuzhbzxcjpvvwpuwbobvtcn"],"signature":"l50Wx8Ayci0hjJqz1hFiK02f/9IZrY2N3yss7q0VfTRTG2tfAGrBEFRyt62GJV7Bl10re1Id2rhDAUJCm3UMAA=="}
                """;

        assertThatNoException().isThrownBy(() -> mapper.readValue(json, TickData.class));
    }

}
