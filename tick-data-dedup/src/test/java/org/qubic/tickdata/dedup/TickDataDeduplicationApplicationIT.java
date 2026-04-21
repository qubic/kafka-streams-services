package org.qubic.tickdata.dedup;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.qubic.tickdata.dedup.model.TickData;
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
        topics = {"tickdata-in", "tickdata-out"},
        controlledShutdown = true
)
@TestPropertySource(properties = {
        "streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "streams.replication-factor=-1",
        "dedup.input-topic=tickdata-in",
        "dedup.output-topic=tickdata-out",
        "streams.state-dir=/tmp/${random.uuid}"
})
public class TickDataDeduplicationApplicationIT {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void testDeduplication() {

        TickData tickData = TickData.builder()
                .computorIndex(257)
                .epoch(209)
                .tickNumber(49485485)
                .timestamp(1776261003000L)
                .signature("sig1")
                .build();

        KafkaTemplate<String, TickData> template = createKafkaTemplate();
        template.send("tickdata-in", "49485485", tickData);
        template.send("tickdata-in", "49485485", tickData);
        template.send("tickdata-in", "49485485", tickData);

        Consumer<String, TickData> consumer = createConsumer();
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "tickdata-out");
        ConsumerRecords<String, TickData> replies = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        
        // Should only have 1 unique record
        assertThat(replies.count()).isEqualTo(1);
        assertThat(replies.iterator().next().value().getTickNumber()).isEqualTo(49485485);
    }

    private Consumer<String, TickData> createConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.embeddedKafka, "test-group", true);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put("spring.json.trusted.packages", "*");
        ConsumerFactory<String, TickData> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        return cf.createConsumer();
    }

    private KafkaTemplate<String, TickData> createKafkaTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        ProducerFactory<String, TickData> pf = new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(pf);
    }

}
