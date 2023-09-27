package br.com.bicmsystems;

import br.com.bicmsystems.consumer.KafkaConsumerData;
import br.com.bicmsystems.consumer.KafkaService;
import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorServiceMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var fraudService = new FraudDetectorServiceMain();
        var groupId = FraudDetectorServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId, "ECOMMERCE_NEW_ORDER", null);

        try(var service = new KafkaService<>(data, fraudService::parse, Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("key: " + record.key() +
                " / value: " + record.value() +
                " / partition: " + record.partition() +
                " / offset: " + record.offset());
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        var message = record.value();
        var order = message.getPayload();
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            if (isFraud(order)) {
                System.out.println("Order is a fraud");
                orderDispatcher.send("ECOMMERCE_ORDER_REJECTED",
                        message.getId().continueWith(FraudDetectorServiceMain.class.getSimpleName()),
                        order.email(), order);
            } else {
                System.out.println("Order accepted: " + record.value());
                orderDispatcher.send("ECOMMERCE_ORDER_APPROVED",
                        message.getId().continueWith(FraudDetectorServiceMain.class.getSimpleName()),
                        order.email(), order);
            }
        }
    }

    public Boolean isFraud(Order order) {
        return order.amount().compareTo(BigDecimal.valueOf(4500L)) >= 0;
    }
}
