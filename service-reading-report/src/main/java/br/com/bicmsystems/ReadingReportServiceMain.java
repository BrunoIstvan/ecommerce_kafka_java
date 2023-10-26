package br.com.bicmsystems;

import br.com.bicmsystems.consumer.ConsumerService;
import br.com.bicmsystems.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.nio.file.Path;

public class ReadingReportServiceMain implements ConsumerService<User> {

    private static final int THREADS = 5;

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {

        new ServiceRunner<>(ReadingReportServiceMain::new).start(THREADS);

    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) throws Exception {

        System.out.println("------------------------------------------");
        var message = record.value();
        var user = message.payload();
        System.out.println("Processing report for " + user.uuid());
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.uuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportServiceMain.class.getSimpleName();
    }

}
