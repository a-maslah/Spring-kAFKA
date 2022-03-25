package com.sid.kafkademo;


import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "test5",groupId = "groupe-ms")
    public void onMessage(ConsumerRecord<String,String> cr) throws Exception {
        System.out.println("*********************");
        PageEvent pageEvent = pageEvent(cr.value());
        System.out.println("Receiving message key=>"+cr.key()+"");
        System.out.println("Value=>"+pageEvent.getPage()+" "+pageEvent.getDuration()+pageEvent.getDate());
        System.out.println("*********************");

    }

    private PageEvent pageEvent(String jsonPageEvent) throws  Exception {
        JsonMapper jsonMapper = new JsonMapper();
        PageEvent  pageEvent = jsonMapper.readValue(jsonPageEvent,PageEvent.class);
        return pageEvent;
    }
}
