package com.github.tomitakussaari.mysql;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionChecker {
    boolean connectionOk(final Connection conn) throws SQLException;
}
