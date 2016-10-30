package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ConnectionAndStatusTest {

    @Test
    public void silentlyClosesConnectionEvenIfItThrowsSQLException() throws SQLException {
        Connection conn = mock(Connection.class);
        ConnectionChecker checker = mock(ConnectionChecker.class);
        ConnectionAndStatus connectionAndStatus = new ConnectionAndStatus(conn, checker);

        doThrow(new SQLException()).when(conn).close();

        connectionAndStatus.close();

        verify(conn).close();
    }

}