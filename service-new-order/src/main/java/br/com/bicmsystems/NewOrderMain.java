package br.com.bicmsystems;

import br.com.bicmsystems.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var orderDispatcher = new KafkaDispatcher<Order>()) {

//            try (var emailDispatcher = new KafkaDispatcher<Email>()) {

                var email = Math.random() + "@email.com";
                for (int i = 0; i < 50; i++) {

                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var order = new Order(orderId, amount, email);
                    var id = new CorrelationId(NewOrderMain.class.getSimpleName());

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", id, email, order);

//                    var emailText = new Email("Reporting status",
//                            "Olá " + email + ", Thank you for your order! We are processing your order!");
//                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", id, email, emailText);

                }

//            }

        }

    }

}
