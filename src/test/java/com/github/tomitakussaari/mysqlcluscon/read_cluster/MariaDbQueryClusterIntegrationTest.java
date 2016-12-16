package com.github.tomitakussaari.mysqlcluscon.read_cluster;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;
import com.github.tomitakussaari.mysqlcluscon.MysclusconDriver.ConnectionType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MariaDbQueryClusterIntegrationTest {

    private static List<DB> dbs = new ArrayList<>();
    private static DBConfigurationBuilder master;
    private static DBConfigurationBuilder slave1;
    private static DBConfigurationBuilder slave2;
    private final Connection masterConn;
    private final Connection slave1Conn;
    private final Connection slave2Conn;

    private final ConnectionType connectionType;

    public MariaDbQueryClusterIntegrationTest(ConnectionType connectionType) throws SQLException {
        this.connectionType = connectionType;
        masterConn = DriverManager.getConnection(master.getURL("test"));
        slave1Conn = DriverManager.getConnection(slave1.getURL("test"));
        slave2Conn = DriverManager.getConnection(slave2.getURL("test"));
    }

    @BeforeClass
    public static void initDatabases() throws ManagedProcessException, SQLException {
        master = createDb(true, 1);
        slave1 = createDb(false, 2);
        slave2 = createDb(false, 3);

        try (Connection mConn = DriverManager.getConnection(master.getURL("test"));
            Connection s1Conn = DriverManager.getConnection(slave1.getURL("test"));
            Connection s2Conn = DriverManager.getConnection(slave2.getURL("test"))
        ) {

            try (PreparedStatement statement = mConn.prepareStatement("SHOW MASTER STATUS");
                 ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String masterFile = resultSet.getString("file");
                    Integer masterPos = resultSet.getInt("Position");
                    joinSlave(s1Conn, masterFile, masterPos);
                    joinSlave(s2Conn, masterFile, masterPos);
                } else {
                    fail("master was not started");
                }
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<ConnectionType> data() {
        return Arrays.asList(ConnectionType.MARIADB_READ_CLUSTER, ConnectionType.MYSQL_READ_CLUSTER);
    }

    @Before
    public void cleanUp() throws SQLException {
        executeStatement("START SLAVE;", slave1Conn);
        executeStatement("START SLAVE;", slave2Conn);
    }

    @Test
    public void choosesCorrectDriver() throws SQLException {
        try(Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
            switch(connectionType) {
                case MARIADB_READ_CLUSTER:
                    assertEquals("MariaDB connector/J", conn.getMetaData().getDriverName());
                    break;
                case MYSQL_READ_CLUSTER:
                    assertEquals("MySQL Connector Java", conn.getMetaData().getDriverName());
                    break;
                default:
                    fail("unkonwn drivertype: "+ connectionType);
            }
        }
    }

    @Test
    public void readClusterConnectionCheckerNoticesIfSlaveGoesDown() throws SQLException {
        ReadClusterConnectionChecker readClusterConnectionChecker = new ReadClusterConnectionChecker(1);
        assertEquals(ConnectionStatus.OK, readClusterConnectionChecker.connectionStatus(slave1Conn));
        executeStatement("STOP SLAVE;", slave1Conn);
        assertEquals(ConnectionStatus.STOPPED, readClusterConnectionChecker.connectionStatus(slave1Conn));
    }

    @Test
    public void mysclusconDriverChoosesAnyValidSlave() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
            assertTrue(conn.isValid(1));
        }
    }

    @Test
    public void mysclusconDriverChoosesSlaveThatIsRunningOverOneThatIsStopped() throws SQLException {
        executeStatement("STOP SLAVE;", slave2Conn);
        for (int i = 0; i < 10; i++) {
            try (Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
                String url = conn.getMetaData().getURL();
                assertTrue(url, url.contains(slave1.getPort() + ""));
            }
        }
    }

    @Test
    public void mysclusconDriverChoosesActiveSlavesFromGivenList() throws SQLException {
        int connectedToSlaveOne = 0;
        int connectedToSlaveTwo = 0;
        for (int i = 0; i < 10; i++) {
            try (Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
                String url = conn.getMetaData().getURL();
                if (url.contains(slave1.getPort() + "")) {
                    connectedToSlaveOne++;
                }
                if (url.contains(slave2.getPort() + "")) {
                    connectedToSlaveTwo++;
                }
            }
        }
        assertTrue("Did not connect to slave one", connectedToSlaveOne > 0);
        assertTrue("Did not connect to slave two", connectedToSlaveTwo > 0);
    }

    @Test
    public void replicationMasterIsConsideredValid() throws SQLException {
        try (Connection conn = DriverManager.getConnection(master.getURL("test"), "root", "")) {
            assertTrue(conn.isValid(1));
        }
    }

    @Test
    public void connectionToStoppedSlaveIsNotValidWhenWantedConnectionStatusIsOk() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl() + "?connectionStatus=OK", "root", "")) {
            assertTrue(conn.isValid(1));
            String url = conn.getMetaData().getURL();
            if (url.contains(slave1.getPort() + "")) {
                executeStatement("STOP SLAVE;", slave1Conn);
            } else {
                executeStatement("STOP SLAVE;", slave2Conn);
            }
            assertFalse(conn.isValid(1));
        }
    }

    @Test
    public void doesNotConnectToStoppedSlaveWhenThereIsRunningSlave() throws SQLException {
        int connectedToSlaveOne = 0;
        executeStatement("STOP SLAVE;", slave1Conn);
        for (int i = 0; i < 10; i++) {
            try (Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
                String url = conn.getMetaData().getURL();
                if (url.contains(slave1.getPort() + "")) {
                    connectedToSlaveOne++;
                }
            }
        }
        assertEquals("should not have connected to slave one because it was stopped", 0, connectedToSlaveOne);
    }

    @Test
    public void transparentlyIgnoresInvalidSlaves() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:myscluscon:mysql:read_cluster://localhost:"
                + slave1.getPort() + ",localhost:" + slave2.getPort() +",localhost:1"+ "/test", "root", "")) {
            assertTrue(conn.isValid(1));
        }
    }


    private String connectionUrl() {
        return connectionType.getUrlPrefix()+"://localhost:" + slave1.getPort() + ",localhost:" + slave2.getPort() + "/test";
    }

    @AfterClass
    public static void closeDatabases() throws SQLException {
        dbs.forEach(db -> {
            try {
                db.stop();
            } catch (ManagedProcessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @After
    public void closeConnections() throws SQLException {
        masterConn.close();
        slave1Conn.close();
        slave2Conn.close();
    }

    private static void joinSlave(Connection slaveConn, String masterFile, Integer masterPos) throws SQLException {
        executeStatement("CHANGE MASTER TO\n" +
                "  MASTER_HOST='localhost',\n" +
                "  MASTER_USER='root',\n" +
                "  MASTER_PASSWORD='',\n" +
                "  MASTER_PORT=" + master.getPort() + ",\n" +
                "  MASTER_LOG_FILE='" + masterFile + "',\n" +
                "  MASTER_LOG_POS=" + masterPos + ",\n" +
                "  MASTER_CONNECT_RETRY=10;", slaveConn);
        executeStatement("START SLAVE;", slaveConn);
    }

    private static void executeStatement(String statement, Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(statement);
        }
    }

    private static DBConfigurationBuilder createDb(boolean logBin, Integer serverId) throws ManagedProcessException {
        DBConfigurationBuilder configurationBuilder = DBConfigurationBuilder.newBuilder();
        configurationBuilder.setPort(0);
        if (logBin) {
            configurationBuilder.addArg("--log-bin");
        }
        configurationBuilder.addArg("--server-id=" + serverId.toString());

        DB db = DB.newEmbeddedDB(configurationBuilder.build());
        dbs.add(db);
        db.start();
        return configurationBuilder;
    }
}
