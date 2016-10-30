package com.github.tomitakussaari.mysqlcluscon;

import java.sql.Connection;
import java.sql.SQLException;

class ConnectionAndStatus {
    public final Connection connection;
    private final ConnectionChecker checker;
    private ConnectionStatus status;

    ConnectionStatus getStatus() {
        if(status == null) {
            status = checker.connectionStatus(connection);
        }
        return status;
    }

    ConnectionAndStatus(Connection connection, ConnectionChecker checker) {
        this.connection = connection;
        this.checker = checker;
    }

    void close() {
        try {
            connection.close();
        } catch(SQLException e) {
            //ignored
        }
    }

}
