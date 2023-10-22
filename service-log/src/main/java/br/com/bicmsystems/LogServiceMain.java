package br.com.bicmsystems;

import br.com.bicmsystems.consumer.KafkaConsumerData;
import br.com.bicmsystems.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogServiceMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var logService = new LogServiceMain();
        var groupId = LogServiceMain.class.getSimpleName();
        var data = KafkaConsumerData.pattern(groupId, Pattern.compile("ECOMMERCE.*"));

        try(var service = new KafkaService<>(
                            data, logService::parse,
                            Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("Log: " + record.topic() + " / " +
                           "key: " + record.key() + " / " +
                           "value: " + record.value() + " / " +
                           "partition: " + record.partition() + " / " +
                           "offset: " + record.offset());
    }

}
