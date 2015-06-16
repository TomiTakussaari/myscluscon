package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

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
        try(Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1", "travis", "")) {
            assumeTrue(conn.isValid(1));
        } catch (Exception ignored) {
            throw new AssumptionViolatedException("Mysql should have been accessible for test to run");
        }
    }
}
