package com.sid.kafkademo;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Date;
import java.util.Random;

@org.springframework.web.bind.annotation.RestController
public class RestController {

    private KafkaTemplate<String,PageEvent> kafkaTemplate;

    public RestController(KafkaTemplate<String, PageEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("send/{page}/{topic}")
    public String send(@PathVariable String page,@PathVariable String topic){
        PageEvent pageEvent = new PageEvent(page,new Date(),new Random().nextInt(1000));
        kafkaTemplate.send(topic,"key"+pageEvent.getPage(),pageEvent);
        return "Message Sent ....";
    }

}
