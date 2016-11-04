package com.github.tomitakussaari.mysqlcluscon;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;


@RequiredArgsConstructor
class ConnectionInfo implements AutoCloseable {
    @Getter
    private final Connection connection;
    private final ConnectionChecker checker;
    @Getter(lazy = true)
    private final ConnectionStatus status = calculateStatus();

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            //ignored
        }
    }

    private ConnectionStatus calculateStatus() {
        return checker.connectionStatus(connection);
    }
}
