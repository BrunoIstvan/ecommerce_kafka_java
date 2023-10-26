package br.com.bicmsystems.ecommerce.database;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:" + name + ".db";
        this.connection = DriverManager.getConnection(url);

    }

    public void createIfNotExists(String sql) {
        try {
            this.connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean update(String statement, String... params) throws SQLException {

        return prepare(statement, params).execute();

    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var prepareStatement = this.connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            prepareStatement.setString(i + 1, params[i]);
        }
        return prepareStatement;
    }

    public ResultSet query(String statement, String... params) throws SQLException {

        return prepare(statement, params).executeQuery();

    }

    public void close() throws SQLException {
        this.connection.close();
    }
}
