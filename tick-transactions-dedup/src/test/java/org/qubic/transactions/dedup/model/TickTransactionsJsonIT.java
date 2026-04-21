package org.qubic.transactions.dedup.model;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.UnrecognizedPropertyException;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class TickTransactionsJsonIT {

    // Use spring bean to make sure it's properly configured
    @Autowired
    private ObjectMapper mapper;

    @Test
    void deserialize_fromJson_failOnUnknown() {
        String json = """
                {
                "epoch":209,
                "tickNumber":49485485,
                "transactions":[],
                "unknown":"should not be ignored"
                }
                """;

        assertThatThrownBy(() -> mapper.readValue(json, TickTransactions.class))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("property \"unknown\"");
    }

    @Test
    void deserialize_fromJson_succeed() {
        String json = """
                {
                    "epoch":209,
                    "tickNumber":49485485,
                    "transactions":[
                        {
                            "hash":"fjemxucngcgcxfiktkllxmfqrxggumhfftvpjsuhmdvmyunazxqluiddpkci",
                            "source":"NPGVYDYUFNOQMBYEZFBIQBIBPLHBJHRCOOKKPFIYPEYTSUBOTHHNMPDDELJH",
                            "destination":"XHUVLMVIXQXCFGCDULCZGCIPVWHBBECNIZBWFBWQFGTQVXAUALSQYHIDSING",
                            "amount":1000,
                            "tickNumber":49485485,
                            "inputType":1,
                            "inputSize":0,
                            "inputData":"",
                            "signature":"l50Wx8Ayci0hjJqz1hFiK02f/9IZrY2N3yss7q0VfTRTG2tfAGrBEFRyt62GJV7Bl10re1Id2rhDAUJCm3UMAA==",
                            "timestamp":1776261003000,
                            "moneyFlew":true
                        }
                    ]
                }
                """;

        assertThatNoException().isThrownBy(() -> mapper.readValue(json, TickTransactions.class));
    }

}
