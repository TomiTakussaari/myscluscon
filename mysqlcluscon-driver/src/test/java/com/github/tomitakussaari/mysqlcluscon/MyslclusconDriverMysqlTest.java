package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.Assert.assertTrue;

public class MyslclusconDriverMysqlTest {

    @Test
    public void canConnectToRealMysql() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:myscluscon:mysql:read_cluster://127.0.0.1/myscluscon_test", "travis", "");
        assertTrue(connection.isValid(100));
    }
}
