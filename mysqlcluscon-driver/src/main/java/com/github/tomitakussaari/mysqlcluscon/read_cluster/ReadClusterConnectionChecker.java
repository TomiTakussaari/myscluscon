package com.github.tomitakussaari.mysqlcluscon.read_cluster;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadClusterConnectionChecker implements ConnectionChecker {

    private final int maxSlaveLag;

    public ReadClusterConnectionChecker(int maxSlaveLag) {
        this.maxSlaveLag = maxSlaveLag;
    }

    public ReadClusterConnectionChecker(Map<String, List<String>> queryParameters) {
        this(getParameter(queryParameters, "maxSlaveLag", 2));
    }

    @Override
    public boolean connectionOk(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            return slaveIsRunning(stmt);
        }
    }

    private static Integer getParameter(Map<String, List<String>> queryParameters, String parameter, Integer defaultValue) {
        return Integer.valueOf(queryParameters.getOrDefault(parameter, new ArrayList<>()).stream().findFirst().orElse(defaultValue.toString()));
    }

    private boolean slaveIsRunning(final Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SHOW SLAVE STATUS")) {
            if(rs.next()) {
                return "Yes".equals(rs.getObject("Slave_IO_Running")) && "Yes".equals(rs.getObject("Slave_SQL_Running")) && rs.getInt("Seconds_Behind_Master") <= this.maxSlaveLag;
            }
            return true; //Assume its master and thus working fine
        }
    }
}
