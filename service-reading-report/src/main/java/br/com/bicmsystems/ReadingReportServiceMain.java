package br.com.bicmsystems;

import br.com.bicmsystems.consumer.KafkaConsumerData;
import br.com.bicmsystems.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportServiceMain {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var reportService = new ReadingReportServiceMain();
        var groupId = ReadingReportServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId, "ECOMMERCE_USER_GENERATE_READING_REPORT", null);

        try(var service = new KafkaService<>(data, reportService::parse, Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {

        System.out.println("------------------------------------------");
        var message = record.value();
        var user = message.getPayload();
        System.out.println("Processing report for " + user.uuid());
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.uuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }

}
