package com.github.tomitakussaari.mysqlcluscon;

import com.github.tomitakussaari.mysqlcluscon.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.ReadClusterConnectionChecker;

import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class MysclusconDriver implements Driver {

    static final Logger LOGGER = Logger.getLogger(MysclusconDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new MysclusconDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String mysqlReadClusterConnectorName = "jdbc:myscluscon:mysql:read_cluster";
    public static final String galeraClusterConnectorName = "jdbc:myscluscon:galera:cluster";

    @Override
    public Connection connect(String jdbcUrl, Properties info) throws SQLException {
        final Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters(jdbcUrl);
        final ConnectionChecker connectionChecker = chooseConnectionChecker(jdbcUrl, queryParameters);
        return createConnectionWrapperHandler(connectionChecker, createActualConnection(jdbcUrl, connectionChecker, info));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(mysqlReadClusterConnectorName) || url.startsWith(galeraClusterConnectorName);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return LOGGER;
    }

    private Connection createActualConnection(String jdbcUrl, ConnectionChecker connectionChecker, Properties info) throws SQLException {
        final List<String> hosts = URLHelpers.getHosts(jdbcUrl);
        return tryToOpenConnectionToValidHost(hosts, connectionChecker, info, jdbcUrl)
                .orElseThrow(() -> new SQLException("Unable to open connection, no valid host found from hosts: "+hosts));
    }

    private Optional<Connection> tryToOpenConnectionToValidHost(List<String> hosts, ConnectionChecker connectionChecker, Properties info, String jdbcUrl) throws SQLException {
        LOGGER.fine("Trying to connect to hosts " + hosts + " from url " + jdbcUrl);
        final List<String> copyOfHosts = new ArrayList<>(hosts);
        Collections.shuffle(copyOfHosts);

        for (String host : copyOfHosts) {
            Optional<Connection> conn = tryConnectingToHost(host, connectionChecker, jdbcUrl, info);
            if (conn.isPresent()) {
                return conn;
            }
        }
        return Optional.empty();
    }

    private Optional<Connection> tryConnectingToHost(String host, ConnectionChecker connectionChecker, String jdbcUrl, Properties info) throws SQLException {
        LOGGER.fine("Trying to connect to host " + host);
        final URL originalUrl = URLHelpers.createConvertedUrl(jdbcUrl);
        final String connectUrl = URLHelpers.constructMysqlConnectUrl(originalUrl, host);
        Connection connection = null;
        try {
            LOGGER.fine("Connecting to " + connectUrl);
            connection = openRealConnection(info, connectUrl);
            if(connectionChecker.connectionOk(connection)) {
                return Optional.of(connection);
            } else {
                connection.close();
            }
        } catch(Exception e) {
            LOGGER.fine("Error while verifying connection " + connectUrl + " " + e.getMessage());
            if(connection != null) {connection.close();}
        }
        return Optional.empty();
    }


    protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
        return DriverManager.getConnection(connectUrl, info);
    }


    private ConnectionChecker chooseConnectionChecker(String jdbcUrl, Map<String, List<String>> queryParameters) {
        String protocol = URLHelpers.getProtocol(jdbcUrl);
        LOGGER.fine("Parsed Protocol: " + protocol + " from url" + jdbcUrl);
        switch (protocol) {
            case mysqlReadClusterConnectorName: return new ReadClusterConnectionChecker(queryParameters);
            case galeraClusterConnectorName:    return new GaleraClusterConnectionChecker();
            default:                            throw new UnsupportedOperationException("Unsupported protocol: "+protocol);
        }
    }

    protected Connection createConnectionWrapperHandler(final ConnectionChecker connectionChecker, Connection actualConnection) {
        return new ConnectionWrapper(actualConnection, connectionChecker);
    }

}
