package com.github.tomitakussaari.mysqlcluscon;

import com.github.tomitakussaari.mysqlcluscon.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.ReadClusterConnectionChecker;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomitakussaari.mysqlcluscon.Params.DEFAULT_CONNECT_TIMEOUT_IN_MS;
import static com.github.tomitakussaari.mysqlcluscon.Params.MYSQL_CONNECT_TIMEOUT_PARAM;

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

    private final ServerBlackList serverBlackList = new ServerBlackList();

    @Override
    public Connection connect(String jdbcUrl, Properties info) throws SQLException {
        if(acceptsURL(jdbcUrl)) {
            URLHelpers.URLInfo connectUrl = URLHelpers.parse(jdbcUrl);
            validateQueryParameters(connectUrl.queryParameters, jdbcUrl);
            final ConnectionStatus wantedConnectionStatus = getWantedConnectionStatus(connectUrl.queryParameters);
            final ConnectionChecker connectionChecker = chooseConnectionChecker(connectUrl, connectUrl.queryParameters);
            return createProxyConnection(connectionChecker, createActualConnection(connectUrl, connectionChecker, info, wantedConnectionStatus), wantedConnectionStatus);
        } else {
            return null;
        }
    }

    private void validateQueryParameters(Map<String, List<String>> queryParameters, String jdbcUrl) {
        if(!queryParameters.containsKey(MYSQL_CONNECT_TIMEOUT_PARAM)) {
            LOGGER.info(() -> "No connect timeout specified for URL: "+jdbcUrl+ " using default: "+DEFAULT_CONNECT_TIMEOUT_IN_MS);
            queryParameters.put(MYSQL_CONNECT_TIMEOUT_PARAM, Collections.singletonList(DEFAULT_CONNECT_TIMEOUT_IN_MS.toString()));
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

    private Connection createActualConnection(URLHelpers.URLInfo jdbcUrl, ConnectionChecker connectionChecker, Properties info, ConnectionStatus leastUsableConnection) throws SQLException {
        final List<String> servers = serverBlackList.filterOutBlacklisted(jdbcUrl.servers);
        return tryToOpenConnectionToValidServer(servers, connectionChecker, info, jdbcUrl, leastUsableConnection)
                .orElseThrow(() -> new SQLException("Unable to open connection, no valid host found from servers: " + servers));
    }

    private Optional<Connection> tryToOpenConnectionToValidServer(List<String> servers, ConnectionChecker connectionChecker,
                                                                Properties info, URLHelpers.URLInfo jdbcUrl,
                                                                ConnectionStatus wantedConnectionStatus) throws SQLException {
        LOGGER.fine(() -> "Trying to connect to servers " + servers + " from url " + jdbcUrl);
        Collections.shuffle(servers);

        List<ConnectionAndStatus> connections = null;
        try {
            connections = servers.stream()
                    .flatMap(server -> tryConnectingToHost(server, jdbcUrl, info))
                    .map(conn -> new ConnectionAndStatus(conn, connectionChecker))
                    .collect(Collectors.toList());
            return findAndRemoveBestConnection(connections, wantedConnectionStatus);
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

    Optional<ConnectionAndStatus> findBestConnection(List<ConnectionAndStatus> connections, ConnectionStatus wantedConnectionStatus) {
        return connections.stream()
            .filter(connectionAndStatus -> connectionAndStatus.getStatus().priority >= wantedConnectionStatus.priority)
            .sorted((left, right) -> right.getStatus().priority.compareTo(left.getStatus().priority))
            .findFirst();
    }

    private Stream<Connection> tryConnectingToHost(String server, URLHelpers.URLInfo jdbcUrl, Properties info) {
        LOGGER.fine(() -> "Trying to connect to host " + server);
        final String connectUrl = jdbcUrl.asJdbcConnectUrl(server);
        try {
            LOGGER.fine(() -> "Connecting to " + connectUrl);
            return Stream.of(openRealConnection(info, connectUrl));
        } catch(Exception e) {
            serverBlackList.blackList(server);
            LOGGER.info(() -> "Error while opening connection " + connectUrl + " " + e.getMessage());
            return Stream.empty();
        }
    }


    protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
        return DriverManager.getConnection(connectUrl, info);
    }


    private ConnectionChecker chooseConnectionChecker(URLHelpers.URLInfo jdbcUrl, Map<String, List<String>> queryParameters) {
        LOGGER.fine(() -> "Parsed Protocol: " + jdbcUrl.protocol + " from url" + jdbcUrl);
        switch (jdbcUrl.protocol) {
            case mysqlReadClusterConnectorName: return new ReadClusterConnectionChecker(queryParameters);
            case galeraClusterConnectorName:    return new GaleraClusterConnectionChecker();
            default:                            throw new UnsupportedOperationException("Unsupported protocol: "+jdbcUrl.protocol);
        }
    }

    Connection createProxyConnection(final ConnectionChecker connectionChecker, Connection actualConnection, final ConnectionStatus wantedConnectionStatus) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Connection.class}, (proxy, method, args) -> {
            switch(method.getName()) {
                case "isValid":
                    return connectionChecker.connectionStatus(actualConnection, (Integer) args[0]).priority >= wantedConnectionStatus.priority;
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


    Collection<String> blackListedServers() {
        return serverBlackList.blackListed();
    }
}
