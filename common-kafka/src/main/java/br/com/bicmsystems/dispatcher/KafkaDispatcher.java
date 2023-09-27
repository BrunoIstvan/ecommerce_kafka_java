package br.com.bicmsystems.dispatcher;

import br.com.bicmsystems.CorrelationId;
import br.com.bicmsystems.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    public void send(String topic, CorrelationId id, String key, T payload) throws ExecutionException, InterruptedException {

        Future<RecordMetadata> future = sendAsync(topic, id, key, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, CorrelationId id, String key, T payload) {
        var value = new Message<>(id.continueWith("_" + topic), payload);

        var record = new ProducerRecord<>(topic, key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sent successfully: " + data.topic() +
                    ":::partition: " + data.partition() +
                    " / offset: " + data.offset() +
                    " / timestamp: " + data.timestamp());
        };

        return producer.send(record, callback);
    }

    private Properties properties() {

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return properties;
    }

    @Override
    public void close() {
        this.producer.close();
    }

}
