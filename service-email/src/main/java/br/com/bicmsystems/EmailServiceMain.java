package br.com.bicmsystems;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;


public class EmailServiceMain {

    public static void main(String[] args) {

        var emailService = new EmailServiceMain();
        var groupId = EmailServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId,"ECOMMERCE_SEND_EMAIL", null);
        try(var kafkaService = new KafkaService<>(data, emailService::parse, Email.class, Map.of())) {
            kafkaService.run();
        }

    }

    private void parse(ConsumerRecord<String, Email> record)  {
        System.out.println("Send email: ");
        System.out.println("key: " + record.key() +
                " / value: { subject: " + record.value().getSubject() + " - body: " + record.value().getBody() + " } " +
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
