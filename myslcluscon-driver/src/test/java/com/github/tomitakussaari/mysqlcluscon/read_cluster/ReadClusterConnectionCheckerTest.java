package com.github.tomitakussaari.mysqlcluscon.read_cluster;

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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReadClusterConnectionCheckerTest {

    @Mock
    Connection conn;
    @Mock
    Statement statement;
    @Mock
    ResultSet resultSet;

    ReadClusterConnectionChecker checker = new ReadClusterConnectionChecker(2);

    @Test
    public void allIsGoodCase() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getObject("Seconds_Behind_Master")).thenReturn("0");
        when(resultSet.next()).thenReturn(true);

        assertTrue("Connection was not valid", checker.connectionOk(conn));
        verify(resultSet).getObject("Slave_IO_Running");
    }

    @Test
    public void connectionReportsItsNotValid() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(false);
        assertFalse(checker.connectionOk(conn));
    }

    @Test
    public void slaveIsLaggingBehind() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.getObject("Slave_IO_Running")).thenReturn("Yes");
        when(resultSet.getObject("Slave_SQL_Running")).thenReturn("Yes");
        when(resultSet.getInt("Seconds_Behind_Master")).thenReturn(3);
        when(resultSet.next()).thenReturn(true);

        assertFalse(checker.connectionOk(conn));
        verify(resultSet).getObject("Slave_IO_Running");
    }

    @Test
    public void isNotSlave() throws SQLException {
        when(conn.createStatement()).thenReturn(statement);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(statement.executeQuery("SHOW SLAVE STATUS")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertTrue(checker.connectionOk(conn));
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

        assertFalse(checker.connectionOk(conn));
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

        assertFalse(checker.connectionOk(conn));
    }

}