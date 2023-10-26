package br.com.bicmsystems.consumer;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() throws Exception {

        var service = factory.create();
        var groupId = service.getConsumerGroup();
        var data = KafkaConsumerData.topic(groupId, service.getTopic());
        try(var kafkaService = new KafkaService<>(data, service::parse, Map.of())) {
            kafkaService.run();
        }
        return null;
    }
}
