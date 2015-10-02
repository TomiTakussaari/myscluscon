package com.github.tomitakussaari.mysqlcluscon;

import java.sql.Connection;

@FunctionalInterface
public interface ConnectionChecker {

    default ConnectionStatus connectionStatus(final Connection conn) {
        return connectionStatus(conn, 1);
    }

    ConnectionStatus connectionStatus(final Connection conn, final int timeoutInSeconds);
}
