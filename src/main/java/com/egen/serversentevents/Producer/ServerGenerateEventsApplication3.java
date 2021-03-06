package com.egen.serversentevents.Producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ServerGenerateEventsApplication3 {

    private static final Logger log = LoggerFactory.getLogger(ServerGenerateEventsApplication3.class.getName());

    private static final String TOPIC = "sse-json-test-topic-100";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID_CONFIG = "trans-string-consumer-egen-new";

    private final KafkaSender<String, JsonNode> sender;
    private final SimpleDateFormat dateFormat;

    public ServerGenerateEventsApplication3(String bootstrapServers){

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        SenderOptions<String, JsonNode> senderOptions = SenderOptions.create(props);
        sender = KafkaSender.create(senderOptions);

        dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    }

    public void close() {
        sender.close();
    }

    public void generateMessages(String topic, int count, CountDownLatch latch) throws InterruptedException, JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        String orderId1 = UUID.randomUUID().toString();
        String orderId2 = UUID.randomUUID().toString();
        String jsonValue1 = "{ \"orderId\":\""+orderId1+"\", \"store\":\"GSV\", \"drop\":\"IL\"}";
        String jsonValue2 = "{ \"orderId\":\""+orderId2+"\", \"store\":\"JSV\", \"drop\":\"NY\"}";
        JsonNode jsonNode1 = objectMapper.readTree(jsonValue1);
        JsonNode jsonNode2 = objectMapper.readTree(jsonValue2);

        sender.<Integer>send(Flux.range(1, count)
                .map(i -> {
                    if(i%2 == 0)
                        return SenderRecord.create(new ProducerRecord<>(topic, "Key_"+i, jsonNode1), i);
                    else
                        return SenderRecord.create(new ProducerRecord<>(topic, "Key_"+i, jsonNode2), i);
                }))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();
                    System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s%n",
                            r.correlationMetadata(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            dateFormat.format(new Date(metadata.timestamp())));
                    latch.countDown();
                });
    }


    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        int count = 2;
        CountDownLatch latch = new CountDownLatch(count);
        ServerGenerateEventsApplication3 producer = new ServerGenerateEventsApplication3(BOOTSTRAP_SERVERS);
        producer.generateMessages(TOPIC, count, latch);
        latch.await(10, TimeUnit.SECONDS);
        producer.close();
    }
}
