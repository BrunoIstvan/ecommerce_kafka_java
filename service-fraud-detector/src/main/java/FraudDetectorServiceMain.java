import br.com.bicmsystems.KafkaConsumerData;
import br.com.bicmsystems.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorServiceMain {

    public static void main(String[] args) {

        var fraudService = new FraudDetectorServiceMain();
        var groupId = FraudDetectorServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId, "ECOMMERCE_NEW_ORDER", null);

        try(var service = new KafkaService<>(data, fraudService::parse, Order.class, Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("key: " + record.key() +
                " / value: { userId: " + record.value().getUserId() + " - " +
                "orderId: " + record.value().getOrderId() + " - " +
                "amount: " + record.value().getAmount() + "} " +
                " / partition: " + record.partition() +
                " / offset: " + record.offset());
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }

}
