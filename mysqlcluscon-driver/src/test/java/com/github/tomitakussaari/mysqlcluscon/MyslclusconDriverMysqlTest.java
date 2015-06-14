package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.Assert.assertTrue;

public class MyslclusconDriverMysqlTest {

    @Test
    public void canConnectToSingleMysqlServerUsingReadClusterDriver() throws Exception {
        assumeMysqlIsAccessible();
        try(Connection connection = DriverManager.getConnection("jdbc:myscluscon:mysql:read_cluster://127.0.0.1/myscluscon_test", "travis", "")) {
            assertTrue(connection.isValid(100));

        }
    }

    @Test
    public void canConnectToSingleMysqlServerUsingGaleraClusterDriver() throws Exception {
        assumeMysqlIsAccessible();
        try(Connection connection = DriverManager.getConnection("jdbc:myscluscon:galera:cluster://127.0.0.1/myscluscon_test", "travis", "")) {
            assertTrue(connection.isValid(100));
        }
    }

    private void assumeMysqlIsAccessible() {
        try(ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("127.0.0.1", 3306));
            throw new AssumptionViolatedException("Mysql should have been accessible for test to run");
        } catch (IOException ignored) {
        }
    }
}
