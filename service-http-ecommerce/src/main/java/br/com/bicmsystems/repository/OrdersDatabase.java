package br.com.bicmsystems.repository;

import br.com.bicmsystems.Order;
import br.com.bicmsystems.ecommerce.database.LocalDatabase;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {

    private final LocalDatabase database;

    public OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        this.database.createIfNotExists("create table Orders (uuid varchar(200) primary key)");
    }

    public boolean saveNewOrder(Order order) throws SQLException {

        if(wasProcessed(order)) {
            return false;
        }
        this.database.update("insert into Orders (uuid) values (?); ", order.orderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {

        return this.database.query("select uuid from Orders where uuid = ? limit 1", order.orderId()).next();

    }

    @Override
    public void close() throws IOException {
        try {
            this.database.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
