package br.com.bicmsystems;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    public KafkaService(KafkaConsumerData data, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, data.getGroupId(), properties);
        if(data.getTopic() != null)
            consumer.subscribe(Collections.singletonList(data.getTopic()));
        else if(data.getPattern() != null)
            consumer.subscribe(data.getPattern());
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        try (var dispatcherDeadLetter = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    System.out.println("-----------------------------------------------");
                    System.out.println("Found " + records.count() + " register(s)");
                    for(var record: records) {
                        try {
                            this.parse.consume(record);
                        } catch (Exception e) {
                            var message = record.value();
                            e.printStackTrace();
                            dispatcherDeadLetter.send("ECOMMERCE_DEADLETTER",
                                    message.getId().continueWith("DeadLetter"),
                                    message.getId().toString(),
                                    new GsonSerializer<>().serialize("", message));
                        }
                    }
                }
            }
        }
    }


    private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }

}