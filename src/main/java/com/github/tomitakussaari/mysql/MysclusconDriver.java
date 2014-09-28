package com.github.tomitakussaari.mysql;

import com.github.tomitakussaari.mysql.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysql.read_cluster.ReadClusterConnectionChecker;
import com.mysql.jdbc.NonRegisteringDriver;

import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import static com.github.tomitakussaari.mysql.URLHelpers.*;

public class MysclusconDriver extends NonRegisteringDriver {

    private static final Logger LOGGER = Logger.getLogger(MysclusconDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new MysclusconDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String mysqlReadClusterConnectorName = "jdbc:myscluscon:mysql:read_cluster";
    public static final String galeraClusterConnectorName = "jdbc:myscluscon:galera:cluster";

    public MysclusconDriver() throws SQLException {
    }

    @Override
    public Connection connect(String jdbcUrl, Properties info) throws SQLException {
        final Map<String, List<String>> queryParameters = getQueryParameters(jdbcUrl);
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
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }

    private Connection createActualConnection(String jdbcUrl, ConnectionChecker connectionChecker, Properties info) throws SQLException {
        final List<String> hosts = getHosts(jdbcUrl);
        Collections.shuffle(hosts);
        return tryToOpenConnectionToValidHost(hosts, connectionChecker, info, jdbcUrl).orElseThrow(() -> new SQLException("Unable to open connection, no valid host found!"));

    }

    private Optional<Connection> tryToOpenConnectionToValidHost(List<String> hosts, ConnectionChecker connectionChecker, Properties info, String jdbcUrl) throws SQLException {
        for(final String host : hosts) {
            final URL originalUrl = createConvertedUrl(jdbcUrl);
            final String connectUrl = constructMysqlConnectUrl(originalUrl, host);
            final Connection connection = openConnection(info, connectUrl);
            try {
                if(connectionChecker.connectionOk(connection)) {
                    return Optional.of(connection);
                } else {
                    connection.close();
                }
            } catch(Exception e) {
                if(connection != null) {connection.close();}
            }
        }
        return Optional.empty();
    }


    protected Connection openConnection(Properties info, String connectUrl) throws SQLException {
        return DriverManager.getConnection(connectUrl, info);
    }


    private ConnectionChecker chooseConnectionChecker(String protocol, Map<String, List<String>> queryParameters) {
        switch (protocol) {
            case mysqlReadClusterConnectorName: return new ReadClusterConnectionChecker(queryParameters);
            case galeraClusterConnectorName:    return new GaleraClusterConnectionChecker();
            default:                            throw new UnsupportedOperationException("Unsupported protocol: "+protocol);
        }
    }

    protected Connection createConnectionWrapperHandler(final ConnectionChecker connectionChecker, Connection actualConnection) {
        return new BasicConnectionWrapper(actualConnection, connectionChecker);
    }

}
