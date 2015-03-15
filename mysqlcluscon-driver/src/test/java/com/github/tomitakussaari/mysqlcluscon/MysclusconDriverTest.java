package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MysclusconDriverTest {

    @Mock
    Connection conn;
    @Mock
    Statement statement;
    @Mock
    ResultSet resultSet;

    TestableMysqlclusconDriver driver = new TestableMysqlclusconDriver();

    @Test
    public void galeraCluster() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("Value")).thenReturn("ON");

        Connection connection = driver.connect("jdbc:myscluscon:galera:cluster://A,B,C", new Properties());
        assertNotNull(connection);
        assertTrue(connection.isValid(1));
        assertTrue(connection instanceof ConnectionWrapper);
    }

    @Test
    public void triesNodesUntilFindsWorkingOneWhenGalera() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(false).thenReturn(false).thenReturn(true);

        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false).thenReturn(false).thenReturn(true);
        when(resultSet.getString("Value")).thenReturn("ON");

        driver.connect("jdbc:myscluscon:galera:cluster://A,B,C", new Properties());
        assertTrue(driver.connectUrls.contains("jdbc:mysql://A"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://B"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://C"));
    }

    @Test
    public void mysqlCluster() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getObject("Seconds_Behind_Master")).thenReturn("0");
        when(resultSet.next()).thenReturn(true);

        Connection connection = driver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C", new Properties());
        assertNotNull(connection);
        assertTrue(connection.isValid(1));
        assertTrue(connection instanceof ConnectionWrapper);
    }

    @Test
    public void triesNodesUntilFindsWorkingOneWhenMysqlCluster() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(false).thenReturn(false).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getObject("Seconds_Behind_Master")).thenReturn("0");
        when(resultSet.next()).thenReturn(true);

        driver.connect("jdbc:myscluscon:mysql:read_cluster://A,B,C", new Properties());
        assertTrue(driver.connectUrls.contains("jdbc:mysql://A"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://B"));
        assertTrue(driver.connectUrls.contains("jdbc:mysql://C"));
    }

    class TestableMysqlclusconDriver extends MysclusconDriver {
        final List<String> connectUrls = new ArrayList<>();
        @Override
        protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
            connectUrls.add(connectUrl);
            return conn;
        }
    }

}