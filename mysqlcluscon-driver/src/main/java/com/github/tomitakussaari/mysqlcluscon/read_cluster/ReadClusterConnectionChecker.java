package com.github.tomitakussaari.mysqlcluscon.read_cluster;

import com.github.tomitakussaari.mysqlcluscon.ConnectionChecker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
                System.out.println(rs.getObject("Slave_IO_Running").toString() + rs.getObject("Slave_SQL_Running") + getSlaveLag(rs));
                return ("Yes".equals(rs.getObject("Slave_IO_Running")) && "Yes".equals(rs.getObject("Slave_SQL_Running")) && getSlaveLag(rs).orElse(Integer.MAX_VALUE) < this.maxSlaveLag);
            }
            return true; //This is probably master, so assume it is running
        }
    }

    private static Optional<Integer> getSlaveLag(final ResultSet rs) {
        try {
            return Optional.of(rs.getInt("Seconds_Behind_Master"));
        } catch (SQLException ex) {
            return Optional.empty();
        }
    }
}
