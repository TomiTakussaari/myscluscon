package com.github.tomitakussaari.mysqlcluscon.galera;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class GaleraClusterConnectionChecker implements ConnectionChecker{

    @Override
    public ConnectionStatus connectionStatus(final Connection conn) {
        try {
            if(! conn.isValid(1)) {
               return ConnectionStatus.DEAD;
            }
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SHOW STATUS like 'wsrep_ready'");
                if(rs.next()) {
                    if("ON".equalsIgnoreCase(rs.getString("Value"))) {
                        return ConnectionStatus.OK;
                    }
                    return ConnectionStatus.STOPPED;
                }
            }
        } catch (Exception e) {
            return ConnectionStatus.DEAD;
        }
        return ConnectionStatus.OK; //not galera, assume OK ?
    }
}
