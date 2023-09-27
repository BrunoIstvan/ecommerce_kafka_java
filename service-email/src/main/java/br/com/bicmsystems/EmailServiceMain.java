package br.com.bicmsystems;

import br.com.bicmsystems.consumer.KafkaConsumerData;
import br.com.bicmsystems.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;


public class EmailServiceMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var emailService = new EmailServiceMain();
        var groupId = EmailServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId,"ECOMMERCE_SEND_EMAIL", null);
        try(var kafkaService = new KafkaService<>(data, emailService::parse, Map.of())) {
            kafkaService.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<Email>> record)  {
        System.out.println("Send email: ");
        var email = record.value().getPayload();
        System.out.println("key: " + record.key() +
                " / value: { subject: " + email.getSubject() + " - body: " + email.getBody() + " } " +
                " / partition: " + record.partition() +
                " / offset: " + record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
