package com.egen.serversentevents.controller;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping(path = "/sse/t2")
public class SSEController2 {

    private static final String STRING_TOPIC = "sse-test-topic-multiple-100";

    /**
    *
    * Static Kafka Consumer configurations.
    * @Param=groupIdConfig
    * */
    private Map<String, Object> kafkaReceiverConfigurations(String groupIdConfig){

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "string-consumer-"+groupIdConfig+"-"+ UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return props;
    }


    /**
    *
    * Handing multiple clients.
    *
    * */
    @GetMapping(value = "/multiple-flux-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getEventsFlux(@RequestParam(name = "id") String groupIdConfig){
        Map<String, Object> propsMaps = this.kafkaReceiverConfigurations(groupIdConfig);
        propsMaps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaReceiver<String,String> kafkaReceiver =
                new DefaultKafkaReceiver(ConsumerFactory.INSTANCE, ReceiverOptions.create(propsMaps).subscription(Collections.singleton(STRING_TOPIC)));

        Flux<ReceiverRecord<String,String>> kafkaFlux = kafkaReceiver.receive();
        return kafkaFlux.log()
                .doOnNext(r -> r.receiverOffset().acknowledge()).map(ReceiverRecord::value)
                .doOnComplete(()-> System.out.println("Streaming completed"))
                .doOnError(error -> System.out.println("Streaming Error: "+error));

    }
}
