package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class MyslclusconDriverMysqlTest {

    @Test
    public void canConnectToRealMysql() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:myscluscon:mysql:read_cluster://127.0.0.1:3306/myscluscon_test", "travis", "");
        assertTrue(connection.isValid(100));
    }
}
