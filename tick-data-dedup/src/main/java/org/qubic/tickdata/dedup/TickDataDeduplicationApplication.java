package org.qubic.tickdata.dedup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class TickDataDeduplicationApplication {

    public static void main(String[] args) {
        SpringApplication.run(TickDataDeduplicationApplication.class, args);
    }

}
