package org.qubic.logs.dedup;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.qubic.logs.dedup.model.EventLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {"log-in", "log-out"},
        controlledShutdown = true
)
@TestPropertySource(properties = {
        "streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "dedup.input-topic=log-in",
        "dedup.output-topic=log-out",
        "streams.state-dir=/tmp/${random.uuid}"
})
public class DeduplicationApplicationIT {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void testDeduplication() {

        EventLog eventLog = EventLog.builder()
                .epoch(123)
                .tickNumber(42)
                .logId(1001)
                .logDigest("digest42")
                .build();

        KafkaTemplate<String, EventLog> template = createKafkaTemplate();
        template.send("log-in", "42", eventLog);
        template.send("log-in", "42", eventLog);
        template.send("log-in", "42", eventLog);

        Consumer<String, EventLog> consumer = createConsumer();
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "log-out");
        ConsumerRecords<String, EventLog> replies = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        assertThat(replies.count()).isGreaterThanOrEqualTo(1);
    }

    private Consumer<String, EventLog> createConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.embeddedKafka, "test-group", true);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put("spring.json.trusted.packages", "*");
        ConsumerFactory<String, EventLog> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        return cf.createConsumer();
    }

    private KafkaTemplate<String, EventLog> createKafkaTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        ProducerFactory<String, EventLog> pf = new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(pf);
    }


}
