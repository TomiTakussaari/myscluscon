package com.github.tomitakussaari.mysqlcluscon.read_cluster;

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
public class ReadClusterConnectionCheckerTest {

    @Mock
    private Connection conn;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;

    private ReadClusterConnectionChecker checker = new ReadClusterConnectionChecker(2);

    @Test
    public void allIsGoodCase() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getObject("Seconds_Behind_Master")).thenReturn("0");
        when(resultSet.next()).thenReturn(true);

        assertEquals("Connection was not valid", ConnectionStatus.OK, checker.connectionStatus(conn));
        verify(resultSet).getObject("Slave_IO_Running");
    }

    @Test
    public void connectionReportsItsNotValid() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(false);
        assertEquals(ConnectionStatus.DEAD, checker.connectionStatus(conn));
    }

    @Test
    public void deadWhenExceptionIsThrownWhenCheckingSlaveStatus() throws SQLException {
        when(conn.isValid(anyInt())).thenReturn(true);
        when(conn.createStatement()).thenThrow(new SQLException(""));
        assertEquals(ConnectionStatus.DEAD, checker.connectionStatus(conn));
    }

    @Test
    public void deadWhenExceptionIsThrownWhenCheckingValidity() throws SQLException {
        when(conn.isValid(anyInt())).thenThrow(new SQLException(""));
        assertEquals(ConnectionStatus.DEAD, checker.connectionStatus(conn));
    }

    @Test
    public void ifThreeSecondsBehindMasterThenSlaveIsLaggingBehind() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getInt("Seconds_Behind_Master")).thenReturn(3);
        when(resultSet.next()).thenReturn(true);

        assertEquals(ConnectionStatus.BEHIND, checker.connectionStatus(conn));
        verify(resultSet).getObject("Slave_IO_Running");
    }

    @Test
    public void ifTwoSecondsBehindMasterThenSlaveIsOK() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getInt("Seconds_Behind_Master")).thenReturn(2);
        when(resultSet.next()).thenReturn(true);

        assertEquals(ConnectionStatus.OK, checker.connectionStatus(conn));
        verify(resultSet).getObject("Slave_IO_Running");
    }

    @Test
    public void isNotSlave() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertEquals(ConnectionStatus.OK, checker.connectionStatus(conn));
    }

    @Test
    public void dbConsideredDeadWhenNoDbPrivilegesToCheckSlaveStatus() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenThrow(new SQLException("No access", "42000"));

        assertEquals(ConnectionStatus.DEAD, checker.connectionStatus(conn));
    }

    @Test
    public void slaveIONotRunning() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("no");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getInt("Seconds_Behind_Master")).thenReturn(0);
        when(resultSet.next()).thenReturn(true);

        assertEquals(ConnectionStatus.STOPPED, checker.connectionStatus(conn));
    }

    @Test
    public void slaveSQLNotRunning() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("no");
        when(resultSet.getInt("Seconds_Behind_Master")).thenReturn(0);
        when(resultSet.next()).thenReturn(true);

        assertEquals(ConnectionStatus.STOPPED, checker.connectionStatus(conn));
    }

}