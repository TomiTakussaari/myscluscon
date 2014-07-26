package com.github.tomitakussaari.mysql.master_slave;

import com.github.tomitakussaari.mysql.ConnectionChecker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ReadClusterConnectionChecker implements ConnectionChecker {

    private final int maxLag;
    private final int maxLocked;
    private final int maxQueries;
    private final int maxThreads;

    public ReadClusterConnectionChecker(int maxLag, int maxLocked, int maxQueries, int maxThreads) {
        this.maxLag = maxLag;
        this.maxLocked = maxLocked;
        this.maxQueries = maxQueries;
        this.maxThreads = maxThreads;
    }

    public ReadClusterConnectionChecker(Map<String, List<String>> queryParameters) {
        this(getParameter(queryParameters, "maxLag", "1"), getParameter(queryParameters, "maxLocked", "1"), getParameter(queryParameters, "maxQueries", "5"), getParameter(queryParameters, "maxThreads", "20"));
    }

    private static Integer getParameter(Map<String, List<String>> queryParameters, String parameter, String defaultValue) {
        return Integer.valueOf(queryParameters.getOrDefault(parameter, new ArrayList<>()).stream().findFirst().orElse(defaultValue));
    }

    @Override
    public boolean connectionOk(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            return slaveStatusOK(stmt) && processListOK(stmt) && threadCountOK(stmt);
        }
    }

    private boolean slaveStatusOK(final Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SHOW SLAVE STATUS")) {
            return !rs.next() || // If no result, its not slave but probably master (or misconfiguration), we consider that OK for now.
                    ("Yes".equals(rs.getObject("Slave_IO_Running")) && "Yes".equals(rs.getObject("Slave_SQL_Running")) && slaveLag(rs).orElse(Integer.MAX_VALUE) < this.maxLag);
        }
    }

    private static Optional<Integer> slaveLag(final ResultSet rs) {
        try {
            return Optional.of(rs.getInt("Seconds_Behind_Master"));
        } catch (SQLException ex) {
            return Optional.empty();
        }
    }

    private boolean processListOK(final Statement stmt) throws SQLException {

        int queries = 0;
        int locked = 0;

        final String sql = "SHOW PROCESSLIST";

        try (ResultSet rs = stmt.executeQuery(sql)) {

            boolean bail = false;
            while (rs.next() && !bail) {

                final Object state = rs.getObject("State");
                final Object command = rs.getObject("Command");

                if ("Locked".equals(state)) {
                    locked++;
                }

                if ("Query".equals(command)) {
                    queries++;
                }
                bail = (locked > this.maxLocked || queries > this.maxQueries);
            }
        }
        return locked <= this.maxLocked && queries <= this.maxQueries;
    }

    private boolean threadCountOK(final Statement stmt) throws SQLException {

        int threads = Integer.MAX_VALUE; // Fear the worst
        try (ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Threads_Running'")) {
            if (rs.next()) {
                threads = rs.getInt("Value");
            }
        }
        return threads <= this.maxThreads;
    }

}
