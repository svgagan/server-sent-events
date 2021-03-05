package com.egen.serversentevents.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@RestController
@RequestMapping(path = "/sse/t1")
public class SSEController1 {

    @Autowired
    KafkaReceiver<String,String> kafkaReceiver;

    /**
    *
    * Handling Single Client.
    *
    * */
    @GetMapping(value = "/flux-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getEventsFlux(){
        Flux<ReceiverRecord<String,String>> kafkaFlux = kafkaReceiver.receive();
        return kafkaFlux.checkpoint("Messages are started being consumed")
                .log()
                .doOnNext(r -> r.receiverOffset().acknowledge()).map(ReceiverRecord::value).checkpoint("Messages are done consumed")
                .doOnComplete(()-> System.out.println("Streaming completed"))
                .doOnError(error -> System.out.println("Streaming Error: "+error));
    }

}
