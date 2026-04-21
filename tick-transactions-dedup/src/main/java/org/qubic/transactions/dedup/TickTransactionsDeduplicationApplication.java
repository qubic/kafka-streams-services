package org.qubic.transactions.dedup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class TickTransactionsDeduplicationApplication {

    public static void main(String[] args) {
        SpringApplication.run(TickTransactionsDeduplicationApplication.class, args);
    }

}
