package com.github.tomitakussaari.mysqlcluscon.galera;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GaleraClusterConnectionCheckerTest {

    @Mock
    Connection conn;
    @Mock
    Statement statement;
    @Mock
    ResultSet resultSet;

    GaleraClusterConnectionChecker clusterConnectionChecker = new GaleraClusterConnectionChecker();

    @Test
    public void allIsGoodCase() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("Value")).thenReturn("ON");
        assertTrue(clusterConnectionChecker.connectionOk(conn));
    }

    @Test
    public void connectionReportsItsNotValid() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(false);
        assertFalse(clusterConnectionChecker.connectionOk(conn));
    }

    @Test
    public void replicationNotRunning() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("Value")).thenReturn("OFF");
        assertFalse(clusterConnectionChecker.connectionOk(conn));
    }

    @Test
    public void serverWithNoGaleraIsConsideredRunning() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
        assertTrue(clusterConnectionChecker.connectionOk(conn));
    }
}