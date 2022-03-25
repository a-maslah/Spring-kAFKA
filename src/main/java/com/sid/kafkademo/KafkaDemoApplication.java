package com.sid.kafkademo;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
@SpringBootApplication
@EnableKafka
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

}
