package com.github.tomitakussaari.mysqlcluscon;

import java.sql.Connection;

@FunctionalInterface
public interface ConnectionChecker {
    ConnectionStatus connectionStatus(final Connection conn);
}
