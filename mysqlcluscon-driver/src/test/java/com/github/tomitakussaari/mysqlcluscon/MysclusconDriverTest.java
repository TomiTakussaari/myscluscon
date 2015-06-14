package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
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
        assertTrue(driver.connectUrls.contains("jdbc:mysql://A:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://B:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://C:1234?foo=bar&bar=foo"));
    }

    @Test
    public void mysqlCluster() throws SQLException {
        when(mockConn.createStatement()).thenReturn(mockStatement);
        when(mockConn.isValid(anyInt())).thenReturn(true);
        when(mockStatement.executeQuery("SHOW SLAVE STATUS")).thenReturn(mockResultSet);
        when(mockResultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(mockResultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(mockResultSet.getObject("Seconds_Behind_Master")).thenReturn("0");
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
        when(mockResultSet.getObject("Seconds_Behind_Master")).thenReturn("0");
        when(mockResultSet.next()).thenReturn(true);

        driver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C:1234?foo=bar&bar=foo", new Properties());
        assertTrue(driver.connectUrls.contains("jdbc:mysql://A:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://B:1234?foo=bar&bar=foo"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://C:1234?foo=bar&bar=foo"));
    }

    class TestableMysqlclusconDriver extends MysclusconDriver {
        final List<String> connectUrls = new ArrayList<>();
        @Override
        protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
            connectUrls.add(connectUrl);
            return mockConn;
        }
    }

}