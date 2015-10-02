package com.github.tomitakussaari.mysqlcluscon.galera;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class GaleraClusterConnectionChecker implements ConnectionChecker{

    @Override
    public ConnectionStatus connectionStatus(final Connection conn, final int queryTimeoutInSeconds) {
        try {
            if(! conn.isValid(queryTimeoutInSeconds)) {
               return ConnectionStatus.DEAD;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(queryTimeoutInSeconds);
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
