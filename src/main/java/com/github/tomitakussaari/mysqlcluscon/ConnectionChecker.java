package com.github.tomitakussaari.mysqlcluscon;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionChecker {
    boolean connectionOk(final Connection conn) throws SQLException;
}
