package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MysclusconDriverTest {

    @Mock
    Connection mockConn;
    @Mock
    Statement mockStatement;
    @Mock
    ResultSet mockResultSet;

    TestableMysqlclusconDriver driver = new TestableMysqlclusconDriver();
    ConfigurableAndTestableMysclusconDriver configurableDriver = new ConfigurableAndTestableMysclusconDriver();

    @Test
    public void galeraCluster() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getString("Value")).thenReturn("ON");

        Connection connection = driver.connect("jdbc:myscluscon:galera:cluster://A,B,C", new Properties());
        assertNotNull(connection);
        assertTrue(connection.isValid(1));

        verifyConnectionIsUsable(connection);
    }

    private void verifyConnectionIsUsable(Connection connection) throws SQLException {
        PreparedStatement mockPs = Mockito.mock(PreparedStatement.class);
        ResultSet mockRs = Mockito.mock(ResultSet.class);
        when(mockConn.prepareStatement("SELECT * FROM FOOBAR")).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        assertSame(mockRs, connection.prepareStatement("SELECT * FROM FOOBAR").executeQuery());
    }

    @Test
    public void triesNodesUntilFindsWorkingOneWhenGalera() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(false).thenReturn(false).thenReturn(true);

        when(mockStatement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false).thenReturn(false).thenReturn(true);
        when(mockResultSet.getString("Value")).thenReturn("ON");

        driver.connect("jdbc:myscluscon:galera:cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
        assertTrue(driver.connectUrls.toString(), driver.connectUrls.contains("jdbc:mysql://A:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.toString(), driver.connectUrls.contains("jdbc:mysql://B:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.toString(), driver.connectUrls.contains("jdbc:mysql://C:1234?foo=bar&bar=foo"));
    }

    @Test
    public void mysqlCluster() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenReturn(mockResultSet);
        when(mockResultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(mockResultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(mockResultSet.getInt("Seconds_Behind_Master")).thenReturn(0);
        when(mockResultSet.next()).thenReturn(true);

        Connection connection = driver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C", new Properties());
        assertNotNull(connection);
        assertTrue(connection.isValid(1));

        verifyConnectionIsUsable(connection);
    }

    @Test
    public void triesNodesUntilFindsWorkingOneWhenMysqlCluster() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(false).thenReturn(false).thenReturn(true);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenReturn(mockResultSet);
        when(mockResultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(mockResultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(mockResultSet.getInt("Seconds_Behind_Master")).thenReturn(0);
        when(mockResultSet.next()).thenReturn(true);

        driver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
        assertTrue(driver.connectUrls.toString(), driver.connectUrls.contains("jdbc:mysql://A:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.toString(), driver.connectUrls.contains("jdbc:mysql://B:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.toString(), driver.connectUrls.contains("jdbc:mysql://C:1234?foo=bar&bar=foo"));
    }

    @Test
    public void closesRealConnectionIfValidityCheckThrowsExceptionDuringConnectionOpening() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenThrow(new RuntimeException("Foobar"));
        try {
            driver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234?foo=bar&bar=foo", new Properties());
            fail("Should not have passed");
        } catch (SQLException e) {
            assertEquals("Unable to open connection, no valid host found from hosts: [A]", e.getMessage());
        }
        verify(mockConn).close();
    }

    @Test
    public void acceptsUrl() throws SQLException {
        assertFalse(driver.acceptsURL("jdbc:mysql://127.0.0.1"));
        assertTrue(driver.acceptsURL(MysclusconDriver.galeraClusterConnectorName + "://127.0.0.1"));
        assertTrue(driver.acceptsURL(MysclusconDriver.mysqlReadClusterConnectorName + "://127.0.0.1"));
    }

    @Test
    public void returnsNullForNormalMysqlConnectUrl() throws SQLException {
        assertNull(driver.connect("jdbc:mysql://A:1234?foo=bar&bar=foo", new Properties()));
    }

    @Test
    public void proxiedConnectionForwardsIsValidCallToConnectionChecker() throws SQLException {
        ConnectionChecker checker = Mockito.mock(ConnectionChecker.class);
        Connection conn = Mockito.mock(Connection.class);
        Connection proxyConnection = driver.createProxyConnection(checker, conn);
        when(checker.connectionStatus(conn, 10)).thenReturn(ConnectionStatus.OK);
        assertTrue(proxyConnection.isValid(10));
        verify(checker).connectionStatus(conn, 10);
    }

    @Test
    public void isNotJdbcCompliant() {
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    public void version() {
        assertEquals(1, driver.getMajorVersion());
        assertEquals(0, driver.getMinorVersion());
    }

    @Test
    public void okConnectionIsPreferredOverAllOthers() throws SQLException {
        mockConnection("jdbc:mysql://B:1234?foo=bar&bar=foo", "lagging", 3, true, true);
        mockConnection("jdbc:mysql://A:1234?foo=bar&bar=foo", "valid", 0, true, true);
        mockConnection("jdbc:mysql://C:1234?foo=bar&bar=foo", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
        assertEquals("valid", connection.toString());
    }

    @Test
    public void laggingConnectionIsPreferredOverStopped() throws SQLException {
        mockConnection("jdbc:mysql://B:1234?foo=bar&bar=foo", "lagging", 3, true, true);
        mockConnection("jdbc:mysql://A:1234?foo=bar&bar=foo", "stopped", 0, true, false);
        mockConnection("jdbc:mysql://C:1234?foo=bar&bar=foo", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
        assertEquals("lagging", connection.toString());
    }

    @Test
    public void stoppedConnectionIsPreferredOverDead() throws SQLException {
        mockConnection("jdbc:mysql://A:1234?foo=bar&bar=foo", "stopped", 0, true, false);
        mockConnection("jdbc:mysql://C:1234?foo=bar&bar=foo", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
        assertEquals("stopped", connection.toString());
    }

    @Test(expected = SQLException.class)
    public void deadConnectionIsNotUsed() throws SQLException {
        mockConnection("jdbc:mysql://C:1234?foo=bar&bar=foo", "broken", 0, false, true);

        configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
    }

    private Connection mockConnection(String url, String name, Integer secondsBehindMaster, boolean isValid, boolean running) throws SQLException {
        Connection connection = Mockito.mock(Connection.class, name);
        Statement mockStatement = Mockito.mock(Statement.class, name);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class, name);

        when(connection.createStatement()).thenReturn(mockStatement);
        when(connection.isValid(anyInt())).thenReturn(isValid);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenReturn(mockResultSet);
        when(mockResultSet.getObject("Slave_IO_Running")).thenReturn(running ? "Yes" : "No");
        when(mockResultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(mockResultSet.getInt("Seconds_Behind_Master")).thenReturn(secondsBehindMaster);
        when(mockResultSet.next()).thenReturn(true);

        configurableDriver.connectUrls.put(url, connection);
        return connection;
    }

    @Test
    public void findsOkConnection() {
        List<ConnectionAndStatus> connections = new ArrayList<>();
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.OK));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.STOPPED));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);
        Optional<ConnectionAndStatus> bestConnection = driver.findBestConnection(connections);
        assertEquals(ConnectionStatus.OK, bestConnection.get().getStatus());
    }

    @Test
    public void findsLaggingConnectionWhenItIsBest() {
        List<ConnectionAndStatus> connections = new ArrayList<>();
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);

        Optional<ConnectionAndStatus> bestConnection = driver.findBestConnection(connections);
        assertEquals(ConnectionStatus.BEHIND, bestConnection.get().getStatus());
    }

    @Test
    public void findsStoppedConnectionWhenItIsBest() {
        List<ConnectionAndStatus> connections = new ArrayList<>();
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);

        Optional<ConnectionAndStatus> bestConnection = driver.findBestConnection(connections);
        assertEquals(ConnectionStatus.STOPPED, bestConnection.get().getStatus());
    }

    @Test
    public void doesNotReturnDeadConnection() {
        List<ConnectionAndStatus> connections = new ArrayList<>();
        connections.add(new ConnectionAndStatus(null, (conn, t) -> ConnectionStatus.DEAD));

        Optional<ConnectionAndStatus> bestConnection = driver.findBestConnection(connections);
        assertFalse(bestConnection.toString(), bestConnection.isPresent());
    }

    class TestableMysqlclusconDriver extends MysclusconDriver {
        final List<String> connectUrls = new ArrayList<>();
        @Override
        protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
            connectUrls.add(connectUrl);
            return mockConn;
        }
    }

    static class ConfigurableAndTestableMysclusconDriver extends MysclusconDriver {
        final Map<String, Connection> connectUrls = new HashMap<>();
        @Override
        protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
            return connectUrls.remove(connectUrl);
        }
    }

}