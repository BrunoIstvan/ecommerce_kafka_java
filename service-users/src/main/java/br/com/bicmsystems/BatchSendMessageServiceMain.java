package br.com.bicmsystems;

import br.com.bicmsystems.consumer.KafkaConsumerData;
import br.com.bicmsystems.consumer.KafkaService;
import br.com.bicmsystems.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageServiceMain {

    private final Connection connection;

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    BatchSendMessageServiceMain() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            this.connection.createStatement()
                    .execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageServiceMain();
        var groupId = BatchSendMessageServiceMain.class.getSimpleName();
        var data = KafkaConsumerData.topic(groupId, "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS");

        try(var service = new KafkaService<>(data, batchService::parse, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, RuntimeException {

        var message = record.value();
        System.out.println("------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: " + message.payload());

        // aqui é possível simular um erro que irá fazer uma mensagem ser enviada para o tópico ECOMMERCE_DEADLETTER
        // if(true) throw new RuntimeException("Erro forçado para testar DEADLETTER_QUEUE");

        for (User user: getAllUsers()) {
            userDispatcher.sendAsync(
                    message.payload(),
                    message.id().continueWith(BatchSendMessageServiceMain.class.getSimpleName()),
                    user.uuid(),
                    user);

            System.out.println("Async sent to user: " + user.uuid());
        }

    }

    private List<User> getAllUsers() throws SQLException {

        var result = connection.prepareStatement("select uuid from Users").executeQuery();
        var users = new ArrayList<User>();

        while(result.next()) {
            users.add(new User(result.getString("uuid")));
        }
        return users;
    }

}
