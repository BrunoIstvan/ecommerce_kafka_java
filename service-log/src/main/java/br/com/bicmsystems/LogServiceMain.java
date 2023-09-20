package br.com.bicmsystems;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogServiceMain {

    public static void main(String[] args) {

        var logService = new LogServiceMain();
        var groupId = LogServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId, null, Pattern.compile("ECOMMERCE.*"));

        try(var service = new KafkaService<>(
                            data, logService::parse, String.class,
                            Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Log: " + record.topic() + " / " +
                           "key: " + record.key() + " / " +
                           "value: " + record.value() + " / " +
                           "partition: " + record.partition() + " / " +
                           "offset: " + record.offset());
    }

}
