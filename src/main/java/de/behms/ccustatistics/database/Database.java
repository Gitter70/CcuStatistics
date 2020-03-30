package de.behms.ccustatistics.database;

import de.behms.ccustatistics.Arguments;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

public class Database implements AutoCloseable {

    private final RowSetFactory rowSetFactory;
    private Connection connection = null;

    public Database(Arguments arguments) throws SQLException, ClassNotFoundException {
        Class.forName("org.h2.Driver");

        // Create RowSetFactory
        rowSetFactory = RowSetProvider.newFactory();

        // Set up connection
        Properties properties = new Properties();
        properties.put("user", arguments.getUser());
        properties.put("password", arguments.getPw());

        connection = DriverManager.getConnection(
                "jdbc:h2:tcp://" + arguments.getHost() + ":" + arguments.getPort()
                + "/" + arguments.getDir() + "/" + arguments.getDb(), properties);
        connection.setAutoCommit(false);
    }

    public final RowSet executeQuery(String sql) throws SQLException {
        CachedRowSet rowSet = rowSetFactory.createCachedRowSet();
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            rowSet.populate(resultSet);
        }
        return rowSet;
    }

    public final boolean execute(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.execute(sql);
    }

    public void print(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + " " + resultSetMetaData.getColumnName(i));
            }
            System.out.println("");
        }
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}
