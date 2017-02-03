package com.github.tomitakussaari.mysqlcluscon.galera;

import ch.vorburger.exec.ManagedProcessException;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.MariaDbQueryClusterIntegrationTest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GaleraClusterIntegrationTest {

    private static int dbPort;
    private final GaleraClusterConnectionChecker checker = new GaleraClusterConnectionChecker();

    @BeforeClass
    public static void before() throws ManagedProcessException {
        dbPort = MariaDbQueryClusterIntegrationTest.createDb(true, 1).getPort();
    }

    @Test
    public void oldGaleraClusterSchemeWithMysqlDriver() throws InterruptedException, SQLException {
        verifyConnectionToGaleraWorks("jdbc:myscluscon:galera:cluster");
    }

    @Test
    public void newGaleraClusterSchemeWithMysqlDriver() throws InterruptedException, SQLException {
        verifyConnectionToGaleraWorks("jdbc:myscluscon:mysql:galera");
    }

    @Test
    public void oldGaleraClusterSchemeWithMariaDbDriver() throws InterruptedException, SQLException {
        verifyConnectionToGaleraWorks("jdbc:myscluscon:mariadb:galera:cluster");
    }

    @Test
    public void newGaleraClusterSchemeWithMariaDbDriver() throws InterruptedException, SQLException {
        verifyConnectionToGaleraWorks("jdbc:myscluscon:mariadb:galera");
    }

    private void verifyConnectionToGaleraWorks(String scheme) throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(scheme + "://localhost:" + dbPort + "/test?connectTimeout=500");
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMaxLifetime(30000);
        hikariConfig.setIdleTimeout(10000);
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);

        try (Connection conn = dataSource.getConnection()) {
            assertTrue(conn.isValid(1));
            ConnectionStatus connectionStatus = checker.connectionStatus(conn, 1);
            assertEquals(ConnectionStatus.STOPPED, connectionStatus); //our "galera cluster" is not running
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        MariaDbQueryClusterIntegrationTest.closeDatabases();
    }
}
