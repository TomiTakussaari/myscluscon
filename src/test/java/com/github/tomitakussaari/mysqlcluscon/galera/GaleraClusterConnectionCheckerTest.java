package com.github.tomitakussaari.mysqlcluscon.galera;

import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GaleraClusterConnectionCheckerTest {

    @Mock
    private Connection conn;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;

    private GaleraClusterConnectionChecker clusterConnectionChecker = new GaleraClusterConnectionChecker();

    @Test
    public void allIsGoodCase() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("Value")).thenReturn("ON");
        assertEquals(ConnectionStatus.OK, clusterConnectionChecker.connectionStatus(conn));
        verify(resultSet).close();
        verify(statement).close();
    }

    @Test
    public void connectionReportsItsNotValid() throws SQLException {
        when(conn.isValid(anyInt())).thenReturn(false);
        assertEquals(ConnectionStatus.DEAD, clusterConnectionChecker.connectionStatus(conn));
    }

    @Test
    public void replicationNotRunning() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("Value")).thenReturn("OFF");
        assertEquals(ConnectionStatus.STOPPED, clusterConnectionChecker.connectionStatus(conn));
        verify(resultSet).close();
        verify(statement).close();

    }

    @Test
    public void serverWithNoGaleraIsConsideredRunning() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW STATUS like 'wsrep_ready'")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
        assertEquals(ConnectionStatus.OK, clusterConnectionChecker.connectionStatus(conn));
    }

    @Test
    public void deadWhenExceptionIsThrownWhenCheckingReplication() throws SQLException {
        when(conn.isValid(anyInt())).thenReturn(true);
        when(conn.createStatement()).thenThrow(new SQLException(""));
        assertEquals(ConnectionStatus.DEAD, clusterConnectionChecker.connectionStatus(conn));
    }

    @Test
    public void deadWhenExceptionIsThrownFromIsValidChek() throws SQLException {
        when(conn.isValid(anyInt())).thenThrow(new SQLException(""));
        assertEquals(ConnectionStatus.DEAD, clusterConnectionChecker.connectionStatus(conn));
    }
}