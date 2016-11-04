package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ConnectionInfoTest {

    @Test
    public void silentlyClosesConnectionEvenIfItThrowsSQLException() throws SQLException {
        Connection conn = mock(Connection.class);
        ConnectionChecker checker = mock(ConnectionChecker.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(conn, checker);

        doThrow(new SQLException()).when(conn).close();

        connectionInfo.close();

        verify(conn).close();
    }

}