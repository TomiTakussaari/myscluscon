package com.github.tomitakussaari.mysqlcluscon.galera;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GaleraClusterConnectionChecker implements ConnectionChecker{

    @Override
    public boolean connectionOk(final Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW STATUS like 'wsrep_ready'");
            if(rs.next()) {
                return "ON".equals(rs.getString("Value"));
            }
        }
        return false;
    }
}
