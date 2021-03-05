package com.egen.serversentevents.configs;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class SpringSSEConfigurations {

    private static final String TOPIC = "sse-test-topic-101";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID_CONFIG = "string-consumer";
    private static final String GROUP_ID_CONFIG = "sse-string-consumer-group-1";

    @Bean
    public KafkaReceiver kafkaReceiver(){

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return new DefaultKafkaReceiver(ConsumerFactory.INSTANCE, ReceiverOptions.create(props).subscription(Collections.singleton(TOPIC)));
    }
}
