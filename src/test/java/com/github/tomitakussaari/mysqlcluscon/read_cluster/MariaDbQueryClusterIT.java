package com.github.tomitakussaari.mysqlcluscon.read_cluster;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MariaDbQueryClusterIT {

    private static List<DB> dbs = new ArrayList<>();
    private static DBConfigurationBuilder master;
    private static DBConfigurationBuilder slave1;
    private static DBConfigurationBuilder slave2;
    private static Connection masterConn;
    private static Connection slave1Conn;
    private static Connection slave2Conn;

    @BeforeClass
    public static void init() throws ManagedProcessException, SQLException {
        master = createDb(true, 1);
        slave1 = createDb(false, 2);
        slave2 = createDb(false, 3);
        masterConn = DriverManager.getConnection(master.getURL("test"));
        slave1Conn = DriverManager.getConnection(slave1.getURL("test"));
        slave2Conn = DriverManager.getConnection(slave2.getURL("test"));
        try(PreparedStatement statement = masterConn.prepareStatement("SHOW MASTER STATUS");
            ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                String masterFile = resultSet.getString("file");
                Integer masterPos = resultSet.getInt("Position");
                joinSlave(slave1Conn, masterFile, masterPos);
                joinSlave(slave2Conn, masterFile, masterPos);
            } else {
                fail("master did not start");
            }
        }
    }

    @Before
    public void cleanUp() throws SQLException {
        executeStatement("START SLAVE;", slave1Conn);
        executeStatement("START SLAVE;", slave2Conn);
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
        try(Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
            assertTrue(conn.isValid(1));
        }
    }

    @Test
    public void mysclusconDriverChoosesSlaveThatIsRunningOverOneThatIsStopped() throws SQLException {
        executeStatement("STOP SLAVE;", slave2Conn);
        for(int i = 0; i < 10; i++) {
            try(Connection conn = DriverManager.getConnection(connectionUrl(), "root", "")) {
                String url = conn.getMetaData().getURL();
                assertTrue(url, url.contains(slave1.getPort()+""));
            }
        }
    }

    @Test
    public void connectionToStoppedSlaveIsNotValidWhenWantedConnectionStatusIsOk() throws SQLException {
        try(Connection conn = DriverManager.getConnection(connectionUrl()+"?connectionStatus=OK", "root", "")) {
            assertTrue(conn.isValid(1));
            String url = conn.getMetaData().getURL();
            if(url.contains(slave1.getPort()+"")) {
                executeStatement("STOP SLAVE;", slave1Conn);
            } else {
                executeStatement("STOP SLAVE;", slave2Conn);
            }
            assertFalse(conn.isValid(1));
        }
    }

    private String connectionUrl() {
        return "jdbc:myscluscon:mysql:read_cluster://localhost:"+slave1.getPort()+",localhost:"+slave2.getPort()+"/test";
    }

    @AfterClass
    public static void close() throws SQLException {
        masterConn.close();
        slave1Conn.close();
        slave2Conn.close();
        dbs.forEach(db -> {
            try {
                db.stop();
            } catch (ManagedProcessException e) {
                throw new RuntimeException(e);
            }
        });
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
        try(Statement st= conn.createStatement()) {
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
