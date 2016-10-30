package com.github.tomitakussaari.mysqlcluscon;

import com.github.tomitakussaari.mysqlcluscon.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.ReadClusterConnectionChecker;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MysclusconDriver implements Driver {

    private static final Logger LOGGER = Logger.getLogger(MysclusconDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new MysclusconDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static final String mysqlReadClusterConnectorName = "jdbc:myscluscon:mysql:read_cluster";
    static final String galeraClusterConnectorName = "jdbc:myscluscon:galera:cluster";

    @Override
    public Connection connect(String jdbcUrl, Properties info) throws SQLException {
        if(acceptsURL(jdbcUrl)) {
            final Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters(jdbcUrl);
            final ConnectionStatus leastUsableConnection = getWantedConnectionStatus(queryParameters);
            final ConnectionChecker connectionChecker = chooseConnectionChecker(jdbcUrl, queryParameters);
            return createProxyConnection(connectionChecker, createActualConnection(jdbcUrl, connectionChecker, info, leastUsableConnection), leastUsableConnection);
        } else {
            return null;
        }
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

    private Connection createActualConnection(String jdbcUrl, ConnectionChecker connectionChecker, Properties info, ConnectionStatus leastUsableConnection) throws SQLException {
        final List<String> hosts = URLHelpers.getHosts(jdbcUrl);
        return tryToOpenConnectionToValidHost(hosts, connectionChecker, info, jdbcUrl, leastUsableConnection)
                .orElseThrow(() -> new SQLException("Unable to open connection, no valid host found from hosts: " + hosts));
    }

    private Optional<Connection> tryToOpenConnectionToValidHost(List<String> hosts, ConnectionChecker connectionChecker, Properties info, String jdbcUrl, ConnectionStatus leastUsableConnection) throws SQLException {
        LOGGER.fine("Trying to connect to hosts " + hosts + " from url " + jdbcUrl);
        final List<String> copyOfHosts = new ArrayList<>(hosts);
        Collections.shuffle(copyOfHosts);

        List<ConnectionAndStatus> connections = null;
        try {
            connections = copyOfHosts.stream()
                    .map(host -> tryConnectingToHost(host, jdbcUrl, info))
                    .filter(Optional::isPresent).map(Optional::get)
                    .map(conn -> new ConnectionAndStatus(conn, connectionChecker))
                    .collect(Collectors.toList());
            return findAndRemoveBestConnection(connections, leastUsableConnection);
        } finally {
            if(connections != null) {
                connections.forEach(ConnectionAndStatus::close);
            }
        }
    }

    private Optional<Connection> findAndRemoveBestConnection(List<ConnectionAndStatus> connections, ConnectionStatus leastUsableConnection) {
        Optional<ConnectionAndStatus> bestConnection = findBestConnection(connections, leastUsableConnection);
        bestConnection.ifPresent(connections::remove);
        return bestConnection.map(conn -> conn.connection);
    }

    Optional<ConnectionAndStatus> findBestConnection(List<ConnectionAndStatus> connections, ConnectionStatus leastUsableConnection) {
        return connections.stream()
            .filter(connectionAndStatus ->connectionAndStatus.getStatus().priority >= leastUsableConnection.priority)
            .sorted((left, right) -> right.getStatus().priority.compareTo(left.getStatus().priority))
            .findFirst();
    }

    private Optional<Connection> tryConnectingToHost(String host, String jdbcUrl, Properties info) {
        LOGGER.fine("Trying to connect to host " + host);
        final URL originalUrl = URLHelpers.createURL(jdbcUrl);
        final String connectUrl = URLHelpers.constructMysqlConnectUrl(originalUrl, host);
        try {
            LOGGER.fine("Connecting to " + connectUrl);
            return Optional.of(openRealConnection(info, connectUrl));
        } catch(Exception e) {
            LOGGER.fine("Error while verifying connection " + connectUrl + " " + e.getMessage());
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

    Connection createProxyConnection(final ConnectionChecker connectionChecker, Connection actualConnection, final ConnectionStatus leastUsableConnection) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Connection.class}, (proxy, method, args) -> {
            switch(method.getName()) {
                case "isValid":
                    return connectionChecker.connectionStatus(actualConnection, (Integer) args[0]).priority >= leastUsableConnection.priority;
                default:
                    return method.invoke(actualConnection, args);
            }
        });
    }

    private ConnectionStatus getWantedConnectionStatus(Map<String, List<String>> queryParameters) {
        return ConnectionStatus.from(
                URLHelpers.getParameter(queryParameters, "connectionStatus", ConnectionStatus.STOPPED.toString())
        ).orElse(ConnectionStatus.STOPPED);
    }

}
