package org.qubic.logs.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.qubic.logs.dedup.model.EventLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.random.RandomGenerator;

public class TestProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        // Send 100 events (with some duplicates)
        for (int i = 0; i < 100; i++) {
            EventLog event = EventLog.builder()
                    .logId(i)
                    .logDigest(UUID.randomUUID().toString().substring(0, 16))
                    .tickNumber(RandomGenerator.getDefault().nextLong())
                    .timestamp(System.currentTimeMillis())
                    .build();

            Map<String, Object> body = new HashMap<>();
            body.put("message", "Test event " + i);
            event.setBody(body);

            String json = mapper.writeValueAsString(event);

            producer.send(new ProducerRecord<>("topic1", event.getLogDigest(), json));

            // Send a duplicate every 10 events
            if (i % 10 == 0) {
                producer.send(new ProducerRecord<>("topic1", event.getLogDigest(), json));
                System.out.println("Sent duplicate for event " + i);
            }

            Thread.sleep(100);
        }

        producer.close();
        System.out.println("Done sending events");
    }
}
