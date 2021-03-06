package com.github.tomitakussaari.mysqlcluscon;

import com.github.tomitakussaari.mysqlcluscon.URLHelpers.URLInfo;
import com.github.tomitakussaari.mysqlcluscon.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysqlcluscon.read_cluster.ReadClusterConnectionChecker;
import com.google.auto.service.AutoService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.github.tomitakussaari.mysqlcluscon.Params.DEFAULT_CONNECT_TIMEOUT_IN_MS;
import static com.github.tomitakussaari.mysqlcluscon.Params.MYSQL_CONNECT_TIMEOUT_PARAM;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@AutoService(java.sql.Driver.class)
public class MysclusconDriver implements Driver {

    private static final Logger LOGGER = Logger.getLogger(MysclusconDriver.class.getName());
    static final String mysqlReadClusterConnectorName = "jdbc:myscluscon:mysql:read_cluster";
    static final String oldGaleraClusterConnectorName = "jdbc:myscluscon:galera:cluster";
    static final String galeraClusterConnectorName = "jdbc:myscluscon:mysql:galera";

    static final String mariadbReadClusterConnectorName = "jdbc:myscluscon:mariadb:read_cluster";
    static final String oldMariadbGaleraClusterConnectorName = "jdbc:myscluscon:mariadb:galera:cluster";
    static final String mariadbGaleraClusterConnectorName = "jdbc:myscluscon:mariadb:galera";

    @RequiredArgsConstructor
    @Getter
    public enum ConnectionType {
        MARIADB_READ_CLUSTER("jdbc:mariadb", singletonList(mariadbReadClusterConnectorName), urlInfo -> new ReadClusterConnectionChecker(urlInfo.queryParameters)),
        MARIADB_GALERA("jdbc:mariadb", asList(oldMariadbGaleraClusterConnectorName, mariadbGaleraClusterConnectorName), urlInfo -> new GaleraClusterConnectionChecker()),
        MYSQL_READ_CLUSTER("jdbc:mysql", singletonList(mysqlReadClusterConnectorName), urlInfo -> new ReadClusterConnectionChecker(urlInfo.queryParameters)),
        MYSQL_GALERA("jdbc:mysql", asList(oldGaleraClusterConnectorName, galeraClusterConnectorName), urlInfo -> new GaleraClusterConnectionChecker());

        private final String driverPrefix;
        private final List<String> urlPrefixes;
        private final ConnectionCheckerSupplier connectionCheckerSupplier;

        static ConnectionType fromProtocol(String protocol) {
            for(ConnectionType driver : ConnectionType.values()) {
                if(driver.getUrlPrefixes().contains(protocol)) {
                    return driver;
                }
            }
            throw new IllegalArgumentException("Unknown protocol: "+protocol);
        }

        @FunctionalInterface
        interface ConnectionCheckerSupplier {
            ConnectionChecker get(URLInfo urlInfo);
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
            URLInfo urlInfo = URLHelpers.parse(jdbcUrl);
            validateQueryParameters(urlInfo.queryParameters, jdbcUrl);
            final ConnectionStatus wantedConnectionStatus = getWantedConnectionStatus(urlInfo.queryParameters);
            final ConnectionChecker connectionChecker = urlInfo.connectionType.getConnectionCheckerSupplier().get(urlInfo);
            final ConnectionInfo connectionInfo = createActualConnection(urlInfo, connectionChecker, info, wantedConnectionStatus);
            return createProxyConnection(connectionChecker, connectionInfo.getConnection(), wantedConnectionStatus, connectionInfo.getStatus());
        } else {
            return null;
        }
    }

    private void validateQueryParameters(Map<String, List<String>> queryParameters, String jdbcUrl) {
        if(!queryParameters.containsKey(MYSQL_CONNECT_TIMEOUT_PARAM)) {
            LOGGER.info(() -> "No connect timeout specified for URL: "+jdbcUrl+ " using default: "+DEFAULT_CONNECT_TIMEOUT_IN_MS);
            queryParameters.put(MYSQL_CONNECT_TIMEOUT_PARAM, singletonList(DEFAULT_CONNECT_TIMEOUT_IN_MS.toString()));
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return Stream.of(ConnectionType.values()).flatMap(ct -> ct.getUrlPrefixes().stream()).anyMatch(url::startsWith);
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

    private ConnectionInfo createActualConnection(URLInfo urlInfo, ConnectionChecker connectionChecker, Properties info, ConnectionStatus leastUsableConnection) throws SQLException {
        final List<String> servers = serverBlackList.withoutBlackListed(urlInfo.servers);
        return tryToOpenConnectionToValidServer(servers, connectionChecker, info, urlInfo, leastUsableConnection)
                .orElseThrow(() -> new SQLException("Unable to open connection, no valid host found from servers: " + servers));
    }

    private Optional<ConnectionInfo> tryToOpenConnectionToValidServer(List<String> servers, ConnectionChecker connectionChecker,
                                                                Properties info, URLInfo urlInfo,
                                                                ConnectionStatus wantedConnectionStatus) throws SQLException {
        LOGGER.fine(() -> "Trying to connect to servers " + servers + " from url " + urlInfo);

        List<ConnectionInfo> activeConnections = new ArrayList<>();
        try {
            for(String server : inRandomOrder(servers)) {
                Optional<ConnectionInfo> conn = tryOpenConnection(connectionChecker, info, urlInfo, server);
                if(isBestPossible(conn)) {
                    return conn;
                } else {
                    addToBlackListIfDownAndReturn(server, conn).ifPresent(activeConnections::add);
                }
            }
            Optional<ConnectionInfo> bestConnection = findBestConnection(activeConnections, wantedConnectionStatus);
            bestConnection.ifPresent(activeConnections::remove);
            return bestConnection;
        } finally {
            activeConnections.forEach(ConnectionInfo::close);
        }
    }

    private List<String> inRandomOrder(List<String> servers) {
        List<String> randomOrderServers = new ArrayList<>(servers);
        Collections.shuffle(randomOrderServers);
        return randomOrderServers;
    }

    private Optional<ConnectionInfo> addToBlackListIfDownAndReturn(String server, Optional<ConnectionInfo> conn) {
        if(conn.map(ConnectionInfo::getStatus).filter(ConnectionStatus.DEAD::equals).isPresent() || !conn.isPresent()) {
            serverBlackList.blackList(server);
        }
        return conn;
    }

    private boolean isBestPossible(Optional<ConnectionInfo> conn) {
        return conn.map(ConnectionInfo::getStatus).filter(status -> status == ConnectionStatus.OK).isPresent();
    }

    private Optional<ConnectionInfo> tryOpenConnection(ConnectionChecker connectionChecker, Properties info, URLInfo urlInfo, String server) {
        return tryConnectingToHost(server, urlInfo, info).map(c -> new ConnectionInfo(c, connectionChecker));
    }

    Optional<ConnectionInfo> findBestConnection(List<ConnectionInfo> connections, ConnectionStatus wantedConnectionStatus) {
        return connections.stream()
            .filter(connectionInfo -> connectionInfo.getStatus().priority >= wantedConnectionStatus.priority)
            .sorted((left, right) -> right.getStatus().priority.compareTo(left.getStatus().priority))
            .findFirst();
    }

    private Optional<Connection> tryConnectingToHost(String server, URLInfo urlInfo, Properties info) {
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

    Connection createProxyConnection(ConnectionChecker connectionChecker, Connection realConn, ConnectionStatus wantedConnectionStatus, ConnectionStatus connectionStatusOnCreate) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Connection.class}, (proxy, method, args) -> {
            if(method.getName().equals("isValid")) {
                ConnectionStatus currentStatus = connectionChecker.connectionStatus(realConn, (Integer) args[0]);
                return currentStatus.priority >= wantedConnectionStatus.priority && currentStatus.priority >= connectionStatusOnCreate.priority;
            } else {
                return method.invoke(realConn, args);
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
