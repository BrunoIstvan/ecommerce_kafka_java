package br.com.bicmsystems;

import br.com.bicmsystems.consumer.KafkaConsumerData;
import br.com.bicmsystems.consumer.KafkaService;
import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailServiceNewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var emailService = new EmailServiceNewOrderMain();
        var groupId = EmailServiceNewOrderMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId, "ECOMMERCE_NEW_ORDER", null);

        try(var service = new KafkaService<>(data, emailService::parse, Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        System.out.println("------------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println("key: " + record.key() +
                " / value: " + record.value() +
                " / partition: " + record.partition() +
                " / offset: " + record.offset());

        var message = record.value();
        var order = message.getPayload();
        var id = message.getId().continueWith(EmailServiceNewOrderMain.class.getSimpleName());
        try(var emailDispatcher = new KafkaDispatcher<Email>()) {
            var emailText = new Email("Reporting status",
                                        "Olá " + order.email() +
                                              ", Thank you for your order! We are processing your order!");
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", id, order.email(), emailText);
        }
    }

}
