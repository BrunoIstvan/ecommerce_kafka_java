package br.com.bicmsystems.consumer;

import br.com.bicmsystems.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {

    void consume(ConsumerRecord<String, Message<T>> record) throws Exception;

}
