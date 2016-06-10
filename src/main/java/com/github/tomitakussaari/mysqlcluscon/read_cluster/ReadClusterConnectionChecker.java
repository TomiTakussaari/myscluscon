package com.github.tomitakussaari.mysqlcluscon.read_cluster;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.ConnectionStatus;
import com.github.tomitakussaari.mysqlcluscon.MysclusconDriver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadClusterConnectionChecker implements ConnectionChecker {

    private static final Logger LOGGER = Logger.getLogger(ReadClusterConnectionChecker.class.getName());
    private final int maxSlaveLag;

    public ReadClusterConnectionChecker(int maxSlaveLag) {
        this.maxSlaveLag = maxSlaveLag;
    }

    public ReadClusterConnectionChecker(Map<String, List<String>> queryParameters) {
        this(getParameter(queryParameters, "maxSlaveLag", 2));
    }

    @Override
    public ConnectionStatus connectionStatus(Connection conn, int timeoutInSeconds) {
        try {
            if(conn.isValid(timeoutInSeconds)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(timeoutInSeconds);
                    return slaveStatus(stmt);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Error while checking connection status for: "+conn, e);
        }
        return ConnectionStatus.DEAD;
    }

    private static Integer getParameter(Map<String, List<String>> queryParameters, String parameter, Integer defaultValue) {
        return Integer.valueOf(queryParameters.getOrDefault(parameter, new ArrayList<>()).stream().findFirst().orElse(defaultValue.toString()));
    }

    private ConnectionStatus slaveStatus(final Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SHOW SLAVE STATUS")) {
            if(rs.next()) {
                final boolean running = "Yes".equals(rs.getObject("Slave_IO_Running")) && "Yes".equals(rs.getObject("Slave_SQL_Running"));
                final boolean notTooMuchBehind = rs.getInt("Seconds_Behind_Master") <= this.maxSlaveLag;
                if(running && notTooMuchBehind) {
                    return ConnectionStatus.OK;
                } else if (running) {
                    return ConnectionStatus.BEHIND;
                } else {
                    return ConnectionStatus.STOPPED;
                }
            }
            return ConnectionStatus.OK; //Assume its master and thus working fine
        }
    }
}
