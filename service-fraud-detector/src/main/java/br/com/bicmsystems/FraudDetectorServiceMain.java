package br.com.bicmsystems;

import br.com.bicmsystems.consumer.ConsumerService;
import br.com.bicmsystems.consumer.ServiceRunner;
import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import br.com.bicmsystems.ecommerce.database.LocalDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorServiceMain implements ConsumerService<Order> {

    private final LocalDatabase database;

    FraudDetectorServiceMain() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (uuid varchar(200) primary key, is_fraud boolean)");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        new ServiceRunner<>(FraudDetectorServiceMain::new).start(1);

    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws Exception {

        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("key: " + record.key() +
                " / value: " + record.value() +
                " / partition: " + record.partition() +
                " / offset: " + record.offset());
        var message = record.value();
        var order = message.payload();

        if(wasProcessed(order)) {
            System.out.println("Order " + order.orderId() + " was already processed.");
            return;
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }

        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            if (isFraud(order)) {

                this.database.update("insert into Orders (uuid, is_fraud) values (?, true); ", order.orderId());

                System.out.println("Order with orderId " + order.orderId() + " is a fraud");
                orderDispatcher.send("ECOMMERCE_ORDER_REJECTED",
                        message.id().continueWith(FraudDetectorServiceMain.class.getSimpleName()),
                        order.email(), order);
            } else {

                this.database.update("insert into Orders (uuid, is_fraud) values (?, false); ", order.orderId());

                System.out.println("Order accepted: " + record.value());
                orderDispatcher.send("ECOMMERCE_ORDER_APPROVED",
                        message.id().continueWith(FraudDetectorServiceMain.class.getSimpleName()),
                        order.email(), order);
            }
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {

        return this.database.query("select uuid from Orders where uuid = ? limit 1", order.orderId()).next();

    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorServiceMain.class.getSimpleName();
    }

    public Boolean isFraud(Order order) {
        return order.amount().compareTo(BigDecimal.valueOf(4500L)) >= 0;
    }
}
