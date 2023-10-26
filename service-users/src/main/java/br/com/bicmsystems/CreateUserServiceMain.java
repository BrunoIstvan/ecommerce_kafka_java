package br.com.bicmsystems;

import br.com.bicmsystems.consumer.ConsumerService;
import br.com.bicmsystems.consumer.ServiceRunner;
import br.com.bicmsystems.ecommerce.database.LocalDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserServiceMain implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserServiceMain() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table Users (uuid varchar(200) primary key, email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserServiceMain::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {

        var order = record.value().payload();
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println("value: " + order);

        if (this.isNewUser(order.email())) {
            String uuid = UUID.randomUUID().toString();
            this.insertNewUser(uuid, order.email());
        }

    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserServiceMain.class.getSimpleName();
    }

    private void insertNewUser(String uuid, String email) throws SQLException {
        this.database.update("insert into Users (uuid, email) values (?, ?)", uuid, email);
        System.out.println("Added user uuid " + uuid + " - e-mail: " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = this.database.query("select uuid from Users where email = ? limit 1", email);
        return !results.next();
    }

}
