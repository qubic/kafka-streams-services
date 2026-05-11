package org.qubic.transactions.dedup;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.qubic.transactions.dedup.model.Transaction;
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
        topics = {"transactions-in", "transactions-out"},
        controlledShutdown = true
)
@TestPropertySource(properties = {
        "streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "streams.replication-factor=-1",
        "dedup.input-topic=transactions-in",
        "dedup.output-topic=transactions-out",
        "streams.state-dir=/tmp/${random.uuid}"
})
public class TransactionDeduplicationApplicationIT {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void testDeduplication() {

        Transaction transaction = Transaction.builder()
                .hash("hash1")
                .source("source1")
                .destination("dest1")
                .signature("sig1")
                .amount(100)
                .tickNumber(49189280)
                .timestamp(1775990260000L)
                .build();

        KafkaTemplate<String, Transaction> template = createKafkaTemplate();
        template.send("transactions-in", "hash1", transaction);
        template.send("transactions-in", "hash1", transaction);
        template.send("transactions-in", "hash1", transaction);

        Consumer<String, Transaction> consumer = createConsumer();
        this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "transactions-out");
        ConsumerRecords<String, Transaction> replies = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

        // Should only have 1 unique record
        assertThat(replies.count()).isEqualTo(1);
        assertThat(replies.iterator().next().value().getHash()).isEqualTo("hash1");
    }

    private Consumer<String, Transaction> createConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.embeddedKafka, "test-group", true);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put("spring.json.trusted.packages", "*");
        ConsumerFactory<String, Transaction> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        return cf.createConsumer();
    }

    private KafkaTemplate<String, Transaction> createKafkaTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        ProducerFactory<String, Transaction> pf = new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(pf);
    }

}
