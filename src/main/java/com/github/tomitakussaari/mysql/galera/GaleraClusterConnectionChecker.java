package com.github.tomitakussaari.mysql.galera;

import com.github.tomitakussaari.mysql.ConnectionChecker;

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
                String status = (String)rs.getObject("Value");
                return "ON".equals(status);
            }
        }
        return false;
    }
}
