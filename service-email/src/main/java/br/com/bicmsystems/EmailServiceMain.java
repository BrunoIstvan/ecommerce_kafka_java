package br.com.bicmsystems;

import br.com.bicmsystems.consumer.ConsumerService;
import br.com.bicmsystems.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class EmailServiceMain implements ConsumerService<Email> {

    private static final int THREADS = 5;

    public static void main(String[] args) {
        new ServiceRunner<>(EmailServiceMain::new).start(THREADS);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public String getConsumerGroup() {
        return EmailServiceMain.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("Send email: ");
        var email = record.value().payload();
        System.out.println("key: " + record.key() +
                " / value: { subject: " + email.subject() + " - body: " + email.body() + " } " +
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
