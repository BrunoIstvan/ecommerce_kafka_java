import br.com.bicmsystems.KafkaConsumerData;
import br.com.bicmsystems.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserServiceMain {

    private final Connection connection;

    CreateUserServiceMain() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            this.connection.createStatement()
                    .execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserServiceMain();
        var groupId = CreateUserServiceMain.class.getSimpleName();
        var data = new KafkaConsumerData(groupId, "ECOMMERCE_NEW_ORDER", null);

        try(var service = new KafkaService<>(data, createUserService::parse, Order.class, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {

        var order = record.value();
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println("value: " + order);

        if (this.isNewUser(order.email())) {
            String uuid = UUID.randomUUID().toString();
            this.insertNewUser(uuid, order.email());
        }

    }

    private void insertNewUser(String uuid, String email) throws SQLException {
        var insert = this.connection.prepareStatement("insert into Users (uuid, email) values (?, ?)");
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("Added user uuid e " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = this.connection.prepareStatement("select uuid from Users where email = ? limit 1");
        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }


}
