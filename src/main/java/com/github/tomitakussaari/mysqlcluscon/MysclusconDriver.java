package com.github.tomitakussaari.mysqlcluscon;

import com.github.tomitakussaari.mysqlcluscon.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.ReadClusterConnectionChecker;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.github.tomitakussaari.mysqlcluscon.Params.DEFAULT_CONNECT_TIMEOUT_IN_MS;
import static com.github.tomitakussaari.mysqlcluscon.Params.MYSQL_CONNECT_TIMEOUT_PARAM;

public class MysclusconDriver implements Driver {

    private static final Logger LOGGER = Logger.getLogger(MysclusconDriver.class.getName());
    static final String mysqlReadClusterConnectorName = "jdbc:myscluscon:mysql:read_cluster";
    static final String galeraClusterConnectorName = "jdbc:myscluscon:galera:cluster";

    static final String mariadbReadClusterConnectorName = "jdbc:myscluscon:mariadb:read_cluster";
    static final String mariadbGaleraClusterConnectorName = "jdbc:myscluscon:mariadb:galera:cluster";

    @RequiredArgsConstructor
    @Getter
    public enum DriverType {
        MARIADB_READ_CLUSTER("jdbc:mariadb", mariadbReadClusterConnectorName, urlInfo -> new ReadClusterConnectionChecker(urlInfo.queryParameters)),
        MARIADB_GALERA("jdbc:mariadb", mariadbGaleraClusterConnectorName, urlInfo -> new GaleraClusterConnectionChecker()),
        MYSQL_READ_CLUSTER("jdbc:mysql", mysqlReadClusterConnectorName, urlInfo -> new ReadClusterConnectionChecker(urlInfo.queryParameters)),
        MYSQL_GALERA("jdbc:mysql", galeraClusterConnectorName, urlInfo -> new GaleraClusterConnectionChecker());

        private final String driverPrefix;
        private final String urlPrefix;
        private final ConnectioncCheckerSupplier connectionCheckerSupplier;

        static DriverType fromProtocol(String protocol) {
            for(DriverType driver : DriverType.values()) {
                if(driver.getUrlPrefix().equals(protocol)) {
                    return driver;
                }
            }
            throw new IllegalArgumentException("Unknown protocol: "+protocol);
        }

        @FunctionalInterface
        interface ConnectioncCheckerSupplier {
            ConnectionChecker get(URLHelpers.URLInfo urlInfo);
        }
    }

    static {
        try {
            DriverManager.registerDriver(new MysclusconDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private final ServerBlackList serverBlackList = new ServerBlackList();

    @Override
    public Connection connect(String jdbcUrl, Properties info) throws SQLException {
        if(acceptsURL(jdbcUrl)) {
            URLHelpers.URLInfo urlInfo = URLHelpers.parse(jdbcUrl);
            validateQueryParameters(urlInfo.queryParameters, jdbcUrl);
            final ConnectionStatus wantedConnectionStatus = getWantedConnectionStatus(urlInfo.queryParameters);
            final ConnectionChecker connectionChecker = urlInfo.driverType.getConnectionCheckerSupplier().get(urlInfo);
            return createProxyConnection(connectionChecker, createActualConnection(urlInfo, connectionChecker, info, wantedConnectionStatus), wantedConnectionStatus);
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
        return Stream.of(DriverType.values()).map(DriverType::getUrlPrefix).anyMatch(url::startsWith);
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

    private Connection createActualConnection(URLHelpers.URLInfo urlInfo, ConnectionChecker connectionChecker, Properties info, ConnectionStatus leastUsableConnection) throws SQLException {
        final List<String> servers = serverBlackList.withoutBlackListed(urlInfo.servers);
        return tryToOpenConnectionToValidServer(servers, connectionChecker, info, urlInfo, leastUsableConnection)
                .orElseThrow(() -> new SQLException("Unable to open connection, no valid host found from servers: " + servers));
    }

    private Optional<Connection> tryToOpenConnectionToValidServer(List<String> servers, ConnectionChecker connectionChecker,
                                                                Properties info, URLHelpers.URLInfo urlInfo,
                                                                ConnectionStatus wantedConnectionStatus) throws SQLException {
        LOGGER.fine(() -> "Trying to connect to servers " + servers + " from url " + urlInfo);

        List<ConnectionInfo> openConnections = new ArrayList<>();
        try {
            for(String server: inRandomOrder(servers)) {
                Optional<ConnectionInfo> conn = tryOpenConnection(connectionChecker, info, urlInfo, server);
                if(isBestPossible(conn)) {
                    return conn.map(ConnectionInfo::getConnection);
                } else {
                    blackListedIfServerDown(server, conn).ifPresent(openConnections::add);
                }
            }
            Optional<ConnectionInfo> bestConnection = findBestConnection(openConnections, wantedConnectionStatus);
            bestConnection.ifPresent(openConnections::remove);
            return bestConnection.map(ConnectionInfo::getConnection);
        } finally {
            openConnections.forEach(ConnectionInfo::close);
        }
    }

    private List<String> inRandomOrder(List<String> servers) {
        List<String> randomOrderServers = new ArrayList<>(servers);
        Collections.shuffle(randomOrderServers);
        return randomOrderServers;
    }

    private Optional<ConnectionInfo> blackListedIfServerDown(String server, Optional<ConnectionInfo> conn) {
        if(conn.map(ConnectionInfo::getStatus).filter(ConnectionStatus.DEAD::equals).isPresent() || !conn.isPresent()) {
            serverBlackList.blackList(server);
        }
        return conn;
    }

    private boolean isBestPossible(Optional<ConnectionInfo> conn) {
        return conn.map(ConnectionInfo::getStatus).filter(status -> status == ConnectionStatus.OK).isPresent();
    }

    private Optional<ConnectionInfo> tryOpenConnection(ConnectionChecker connectionChecker, Properties info, URLHelpers.URLInfo urlInfo, String server) {
        return tryConnectingToHost(server, urlInfo, info).map(c -> new ConnectionInfo(c, connectionChecker));
    }

    Optional<ConnectionInfo> findBestConnection(List<ConnectionInfo> connections, ConnectionStatus wantedConnectionStatus) {
        return connections.stream()
            .filter(connectionInfo -> connectionInfo.getStatus().priority >= wantedConnectionStatus.priority)
            .sorted((left, right) -> right.getStatus().priority.compareTo(left.getStatus().priority))
            .findFirst();
    }

    private Optional<Connection> tryConnectingToHost(String server, URLHelpers.URLInfo urlInfo, Properties info) {
        LOGGER.fine(() -> "Trying to connect to host " + server);
        final String connectUrl = urlInfo.asJdbcConnectUrl(server);
        try {
            LOGGER.fine(() -> "Connecting to " + connectUrl);
            return Optional.of(openRealConnection(info, connectUrl));
        } catch(Exception e) {
            LOGGER.info(() -> "Error while opening connection " + connectUrl + " " + e.getMessage());
            return Optional.empty();
        }
    }


    protected Connection openRealConnection(Properties info, String connectUrl) throws SQLException {
        return DriverManager.getConnection(connectUrl, info);
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
