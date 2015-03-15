package com.github.tomitakussaari.mysql.mysqlcluscon;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class Main {

    public static void main(String...args) throws Exception {
        final String jdbcUrl = args[0];
        final String userName = args[1];
        final String password = args[2];
        //Class.forName("com.github.tomitakussaari.mysqlcluscon.MysclusconDriver");
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);
        hikariConfig.setConnectionTimeout(1000);
        HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
        List<Connection> openedConnections = new ArrayList<>();

        System.out.println("WAITING..");
        Thread.sleep(5000);

        IntStream.range(0, 10).forEach(val -> openedConnections.add(getConnection(hikariDataSource)));

        openedConnections.forEach(Main::dumpConnAndClose);

        hikariDataSource.close();
    }

    private static void dumpConnAndClose(Connection conn) {
        try {
            System.out.println("Conn: "+conn);
            System.out.println("URL: "+ conn.getMetaData().getURL());
            System.out.println("Valid:"+ conn.isValid(1000));
            conn.close();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static Connection getConnection(HikariDataSource hikariDataSource) {
        try {
            return hikariDataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
