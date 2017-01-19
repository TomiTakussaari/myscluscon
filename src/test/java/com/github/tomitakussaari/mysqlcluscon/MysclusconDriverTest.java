package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.*;

@RunWith(MockitoJUnitRunner.class)
public class MysclusconDriverTest {

    @Mock
    private Connection mockConn;
    @Mock
    private Statement mockStatement;
    @Mock
    private ResultSet mockResultSet;

    private ConnectURLStoringDriver driver = new ConnectURLStoringDriver();
    private ConnectionExpectingDriver configurableDriver = new ConnectionExpectingDriver();

    @Test
    public void galeraCluster() throws SQLException {
        mockGaleraHealthChek();

        Connection connection = driver.connect("jdbc:myscluscon:galera:cluster://A,B,C/", new Properties());
        assertThat(connection).isNotNull();
        assertThat(connection.isValid(1)).isTrue();

        verifyConnectionIsUsable(connection);
    }

    private void mockGaleraHealthChek() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getString("Value")).thenReturn("ON");
    }

    private void verifyConnectionIsUsable(Connection connection) throws SQLException {
        PreparedStatement mockPs = Mockito.mock(PreparedStatement.class);
        ResultSet mockRs = Mockito.mock(ResultSet.class);
        when(mockConn.prepareStatement("SELECT * FROM FOOBAR")).thenReturn(mockPs);
        when(mockPs.executeQuery()).thenReturn(mockRs);
        assertThat(connection.prepareStatement("SELECT * FROM FOOBAR").executeQuery()).isSameAs(mockRs);
    }

    @Test
    public void canBeLoadedWithServiceLoader() {
        ServiceLoader<Driver> allDrivers = ServiceLoader.load(Driver.class);
        assertThat(allDrivers).hasAtLeastOneElementOfType(MysclusconDriver.class);
    }

    @Test
    public void returnsFirstGaleraConnectionThatIsWorking() throws SQLException {
        mockGaleraHealthChek();

        driver.connect("jdbc:myscluscon:galera:cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo", new Properties());
        assertThat(driver.connectUrls).hasSize(1);
    }

    @Test
    public void mysqlCluster() throws SQLException {
        mockMysqlReadClusterHealthCheck();

        Connection connection = driver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C/", new Properties());
        assertThat(connection).isNotNull();
        assertThat(connection.isValid(1)).isTrue();

        verifyConnectionIsUsable(connection);
    }

    private void mockMysqlReadClusterHealthCheck() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenReturn(mockResultSet);
        when(mockResultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(mockResultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(mockResultSet.getInt("Seconds_Behind_Master")).thenReturn(0);
        when(mockResultSet.next()).thenReturn(true);
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

        driver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo", new Properties());
        assertThat(driver.connectUrls.contains("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectTimeout=500")).isTrue();
        assertThat(driver.connectUrls.contains("jdbc:mysql://B:1234/?foo=bar&bar=foo&connectTimeout=500")).isTrue();
        assertThat(driver.connectUrls.contains("jdbc:mysql://C:1234/?foo=bar&bar=foo&connectTimeout=500")).isTrue();
    }

    @Test
    public void closesRealConnectionIfValidityCheckThrowsExceptionDuringConnectionOpening() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenThrow(new RuntimeException("Foobar"));
        try {
            driver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234/?foo=bar&bar=foo", new Properties());
            fail("Should not have passed");
        } catch (SQLException e) {
            assertEquals("Unable to open connection, no valid host found from servers: [A:1234]", e.getMessage());
        }
        verify(mockConn).close();
    }

    @Test
    public void acceptsUrl() throws SQLException {
        assertThat(driver.acceptsURL("jdbc:mysql://127.0.0.1")).isFalse();
        assertThat(driver.acceptsURL(MysclusconDriver.galeraClusterConnectorName + "://127.0.0.1")).isTrue();
        assertThat(driver.acceptsURL(MysclusconDriver.mysqlReadClusterConnectorName + "://127.0.0.1")).isTrue();
    }

    @Test
    public void returnsNullForNormalMysqlConnectUrl() throws SQLException {
        assertThat(driver.connect("jdbc:mysql://A:1234?foo=bar&bar=foo", new Properties())).isNull();
    }

    @Test
    public void proxiedConnectionForwardsIsValidCallToConnectionChecker() throws SQLException {
        ConnectionChecker checker = Mockito.mock(ConnectionChecker.class);
        Connection conn = Mockito.mock(Connection.class);
        Connection proxyConnection = driver.createProxyConnection(checker, conn, ConnectionStatus.STOPPED, ConnectionStatus.STOPPED);
        when(checker.connectionStatus(conn, 10)).thenReturn(ConnectionStatus.OK);
        assertThat(proxyConnection.isValid(10)).isTrue();
        verify(checker).connectionStatus(conn, 10);
    }

    @Test
    public void connectionWithStatusBehindIsNotValidWhenWeWantAtleastOk() throws SQLException {
        ConnectionChecker checker = Mockito.mock(ConnectionChecker.class);
        Connection conn = Mockito.mock(Connection.class);
        Connection proxyConnection = driver.createProxyConnection(checker, conn, ConnectionStatus.OK, ConnectionStatus.BEHIND);
        when(checker.connectionStatus(conn, 10)).thenReturn(ConnectionStatus.BEHIND);
        assertThat(proxyConnection.isValid(10)).isFalse();
        verify(checker).connectionStatus(conn, 10);
    }

    @Test
    public void connectionWithStatusBehindIsValidWhenWeWantAtleastBehindAndItWasBehindOnOpen() throws SQLException {
        ConnectionChecker checker = Mockito.mock(ConnectionChecker.class);
        Connection conn = Mockito.mock(Connection.class);
        Connection proxyConnection = driver.createProxyConnection(checker, conn, ConnectionStatus.BEHIND, ConnectionStatus.BEHIND);
        when(checker.connectionStatus(conn, 10)).thenReturn(ConnectionStatus.BEHIND);
        assertThat(proxyConnection.isValid(10)).isTrue();
        verify(checker).connectionStatus(conn, 10);
    }

    @Test
    public void connectionWithStatusStoppedIsNotValidWhenWeWantAtleastBehindAndItWasOKOnOpen() throws SQLException {
        ConnectionChecker checker = Mockito.mock(ConnectionChecker.class);
        Connection conn = Mockito.mock(Connection.class);
        Connection proxyConnection = driver.createProxyConnection(checker, conn, ConnectionStatus.BEHIND, ConnectionStatus.OK);
        when(checker.connectionStatus(conn, 10)).thenReturn(ConnectionStatus.STOPPED);
        assertThat(proxyConnection.isValid(10)).isFalse();
        verify(checker).connectionStatus(conn, 10);
    }

    @Test
    public void connectionWithStatusBehindIsNotValidIfItWasOriginallyOkEvenIfWeWantAtleastBehind() throws SQLException {
        //so that "best possible" connection is always used (some other host might be OK)
        ConnectionChecker checker = Mockito.mock(ConnectionChecker.class);
        Connection conn = Mockito.mock(Connection.class);
        Connection proxyConnection = driver.createProxyConnection(checker, conn, ConnectionStatus.BEHIND, ConnectionStatus.OK);
        when(checker.connectionStatus(conn, 10)).thenReturn(ConnectionStatus.BEHIND);
        assertThat(proxyConnection.isValid(10)).isFalse();
        verify(checker).connectionStatus(conn, 10);
    }

    @Test
    public void jdbcCompliantWorksSameAsMysql() throws SQLException {
        com.mysql.jdbc.Driver mysqlDriver = new com.mysql.jdbc.Driver();
        assertEquals(mysqlDriver.jdbcCompliant(), driver.jdbcCompliant());
    }

    @Test
    public void version() {
        assertEquals(1, driver.getMajorVersion());
        assertEquals(0, driver.getMinorVersion());
    }

    @Test
    public void getPropertyInfoIsNotReallySupported() throws SQLException {
        //we do not support it for now atleast
        assertEquals(0, driver.getPropertyInfo("", null).length);
    }

    @Test
    public void okConnectionIsPreferredOverAllOthers() throws SQLException {
        expectConnection("jdbc:mysql://B:1234/?foo=bar&bar=foo&connectTimeout=500", "lagging", 3, true, true);
        expectConnection("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectTimeout=500", "valid", 0, true, true);
        expectConnection("jdbc:mysql://C:1234/?foo=bar&bar=foo&connectTimeout=500", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo", new Properties());
        assertEquals("valid", connection.toString());
    }

    @Test
    public void laggingConnectionIsPreferredOverStopped() throws SQLException {
        expectConnection("jdbc:mysql://B:1234/?foo=bar&bar=foo&connectTimeout=500", "lagging", 3, true, true);
        expectConnection("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectTimeout=500", "stopped", 0, true, false);
        expectConnection("jdbc:mysql://C:1234/?foo=bar&bar=foo&connectTimeout=500", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo", new Properties());
        assertEquals("lagging", connection.toString());
    }

    @Test(expected = SQLException.class)
    public void stoppedConnectionIsNotReturnedWhenWeWantAtleastBehind() throws SQLException {
        expectConnection("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectionStatus=behind", "stopped", 0, true, false);
        expectConnection("jdbc:mysql://C:1234/?foo=bar&bar=foo&connectionStatus=behind", "broken", 0, false, true);

        configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo&connectionStatus=behind", new Properties());
    }

    @Test
    public void returnsConnectionWithStatusEqualOrAboveWanted() throws SQLException {
        expectConnection("jdbc:mysql://B:1234/?foo=bar&bar=foo&connectionStatus=behind&connectTimeout=500", "lagging", 3, true, true);
        expectConnection("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectionStatus=behind&connectTimeout=500", "stopped", 0, true, false);
        expectConnection("jdbc:mysql://C:1234/?foo=bar&bar=foo&connectionStatus=behind&connectTimeout=500", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo&connectionStatus=behind", new Properties());
        assertEquals("lagging", connection.toString());
    }

    @Test
    public void stoppedConnectionIsPreferredOverDead() throws SQLException {
        expectConnection("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectTimeout=500", "stopped", 0, true, false);
        expectConnection("jdbc:mysql://C:1234/?foo=bar&bar=foo&connectTimeout=500", "broken", 0, false, true);

        Connection connection = configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo", new Properties());
        assertEquals("stopped", connection.toString());
    }

    @Test(expected = SQLException.class)
    public void deadConnectionIsNotUsed() throws SQLException {
        expectConnection("jdbc:mysql://C:1234/?foo=bar&bar=foo", "broken", 0, false, true);

        configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234,C:1234/?foo=bar&bar=foo", new Properties());
    }

    private Connection expectConnection(String url, String name, Integer secondsBehindMaster, boolean isValid, boolean running) throws SQLException {
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

        expectConnection(url, () -> connection);
        return connection;
    }

    @Test
    public void findsOkConnection() {
        List<ConnectionInfo> connections = new ArrayList<>();
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.OK));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.STOPPED));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);
        Optional<ConnectionInfo> bestConnection = driver.findBestConnection(connections, ConnectionStatus.STOPPED);
        assertEquals(ConnectionStatus.OK, bestConnection.get().getStatus());
    }

    @Test
    public void findsLaggingConnectionWhenItIsBest() {
        List<ConnectionInfo> connections = new ArrayList<>();
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);

        Optional<ConnectionInfo> bestConnection = driver.findBestConnection(connections, ConnectionStatus.STOPPED);
        assertEquals(ConnectionStatus.BEHIND, bestConnection.get().getStatus());
    }

    @Test
    public void doesNotReturnLaggingConnectionWhenWeWantAtleastOk() {
        List<ConnectionInfo> connections = new ArrayList<>();
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);

        Optional<ConnectionInfo> bestConnection = driver.findBestConnection(connections, ConnectionStatus.OK);
        assertThat(bestConnection.isPresent()).isFalse();
    }

    @Test
    public void returnsLaggingConnectionWhenWeWantAtleastitAndItIsBestAvailable() {
        List<ConnectionInfo> connections = new ArrayList<>();
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.BEHIND));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);

        Optional<ConnectionInfo> bestConnection = driver.findBestConnection(connections, ConnectionStatus.BEHIND);
        assertEquals(ConnectionStatus.BEHIND, bestConnection.get().getStatus());
    }

    @Test
    public void findsStoppedConnectionWhenItIsBest() {
        List<ConnectionInfo> connections = new ArrayList<>();
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.STOPPED));
        Collections.shuffle(connections);

        Optional<ConnectionInfo> bestConnection = driver.findBestConnection(connections, ConnectionStatus.STOPPED);
        assertEquals(ConnectionStatus.STOPPED, bestConnection.get().getStatus());
    }

    @Test
    public void doesNotReturnDeadConnection() {
        List<ConnectionInfo> connections = new ArrayList<>();
        connections.add(new ConnectionInfo(null, (conn, t) -> ConnectionStatus.DEAD));

        Optional<ConnectionInfo> bestConnection = driver.findBestConnection(connections, ConnectionStatus.STOPPED);
        assertThat(bestConnection.isPresent()).isFalse();
    }

    @Test
    public void setsConnectTimeoutWhenNotSpecified() throws SQLException {
        mockGaleraHealthChek();

        driver.connect("jdbc:myscluscon:galera:cluster://A:1234/?foo=bar&bar=foo", new Properties());
        String actualUrl = driver.connectUrls.get(0);
        assertEquals("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectTimeout=500", actualUrl);
    }

    @Test
    public void usesConnectTimeoutFromUrlWhenGiven() throws SQLException {
        mockGaleraHealthChek();

        driver.connect("jdbc:myscluscon:galera:cluster://A:1234/?foo=bar&bar=foo&connectTimeout=1500", new Properties());
        String actualUrl = driver.connectUrls.get(0);
        assertEquals("jdbc:mysql://A:1234/?foo=bar&bar=foo&connectTimeout=1500", actualUrl);

    }

    @Test
    public void skipsServersThatAreDownFromSubsequentConnectionAttempts() throws SQLException {
        expectConnection("jdbc:mysql://A:1234/?connectTimeout=500", () -> {
            throw new RuntimeException("Cannot open connection");
        });
        try {
            configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234/", new Properties());
            fail("should have thrown SQLException when connecting to non-working server");
        } catch (SQLException e) {
            assertEquals("Unable to open connection, no valid host found from servers: [A:1234]", e.getMessage());
        }
        assertEquals("A:1234", configurableDriver.blackListedServers().iterator().next());

        expectConnection("jdbc:mysql://A:1234/?connectTimeout=500", () -> {
            throw new RuntimeException("Cannot open connection");
        });
        expectConnection("jdbc:mysql://B:1234/?connectTimeout=500", "valid-conn", 0, true, true);
        configurableDriver.connect("jdbc:myscluscon:mysql:read_cluster://A:1234,B:1234/", new Properties());

        assertEquals(configurableDriver.connectionsToConnect.toString(), 1, configurableDriver.connectionsToConnect.size());
    }

    @Test
    public void parentLoggerIsReturned() {
        assertThat(driver.getParentLogger()).isNotNull();
    }

    @Test(expected = IllegalArgumentException.class)
    public void driverTypeForUnknownValue() {
        MysclusconDriver.ConnectionType.fromProtocol("unknown");
    }

    private void expectConnection(String key, Supplier<Connection> connectionSupplier) {
        configurableDriver.connectionsToConnect.put(key, connectionSupplier);
    }

    class ConnectURLStoringDriver extends MysclusconDriver {
        final List<String> connectUrls = new ArrayList<>();

        @Override
        protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
            connectUrls.add(connectUrl);
            return mockConn;
        }
    }

    static class ConnectionExpectingDriver extends MysclusconDriver {
        final Map<String, Supplier<Connection>> connectionsToConnect = new HashMap<>();

        @Override
        protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
            return connectionsToConnect.remove(connectUrl).get();
        }
    }

}