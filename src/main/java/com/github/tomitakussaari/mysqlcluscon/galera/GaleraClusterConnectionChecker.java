package com.github.tomitakussaari.mysqlcluscon.galera;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.ReadClusterConnectionChecker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GaleraClusterConnectionChecker implements ConnectionChecker {

    private static final Logger LOGGER = Logger.getLogger(ReadClusterConnectionChecker.class.getName());

    @Override
    public ConnectionStatus connectionStatus(final Connection conn, final int queryTimeoutInSeconds) {
        try {
            if(! conn.isValid(queryTimeoutInSeconds)) {
               return ConnectionStatus.DEAD;
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(queryTimeoutInSeconds);
                try(ResultSet rs = stmt.executeQuery("SHOW STATUS like 'wsrep_ready'")) {
                    if(rs.next()) {
                        if("ON".equalsIgnoreCase(rs.getString("Value"))) {
                            return ConnectionStatus.OK;
                        }
                        return ConnectionStatus.STOPPED;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Error while checking connection status for: "+conn, e);
            return ConnectionStatus.DEAD;
        }
        return ConnectionStatus.OK; //not galera, assume OK ?
    }
}
