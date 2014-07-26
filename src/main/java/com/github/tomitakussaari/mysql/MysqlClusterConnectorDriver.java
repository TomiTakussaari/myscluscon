package com.github.tomitakussaari.mysql;

import com.github.tomitakussaari.mysql.galera.GaleraClusterConnectionChecker;
import com.github.tomitakussaari.mysql.master_slave.ReadClusterConnectionChecker;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import static com.github.tomitakussaari.mysql.URLHelpers.*;

public class MysqlClusterConnectorDriver implements Driver {

    private final Logger logger = Logger.getLogger(MysqlClusterConnectorDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new MysqlClusterConnectorDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String mysqlReadClusterConnectorName = "jdbc:tomitakussaari:mysql:read_cluster";
    public static final String galeraClusterConnectorName = "jdbc:tomitakussaari:galera:cluster";

    @Override
    public Connection connect(String jdbcUrl, Properties info) throws SQLException {
        final Map<String, List<String>> queryParameters = getQueryParameters(jdbcUrl);
        final ConnectionChecker connectionChecker = chooseConnectionChecker(jdbcUrl, queryParameters);
        final InvocationHandler connectionProxyWrapper = createProxyWrapper(connectionChecker, createActualConnection(jdbcUrl, connectionChecker, info));

        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Connection.class}, connectionProxyWrapper);
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
        return logger;
    }

    private Connection createActualConnection(String jdbcUrl, ConnectionChecker connectionChecker, Properties info) throws SQLException {
        //1. Create list of defined servers in this url
        //2. Go through list in random order and select first that passes connectionChecker
        //3. return it
        //4. if none pass, throw exception or return any (depending on parameters given)
        List<String> hosts = getHosts(jdbcUrl);
        Collections.shuffle(hosts);
        for(String host : hosts) {
            URL originalUrl = createConvertedUrl(jdbcUrl);
            String connectUrl = constructMysqlConnectUrl(originalUrl, host);
            final Connection connection = openConnection(info, connectUrl);
            try {
                if(connectionChecker.connectionOk(connection)) {
                    return connection;
                } else {
                    connection.close();
                }
            } catch(Exception e) {
                if(connection != null) {connection.close();}
            }
        }
        throw new SQLException("Unable to open connection, no valid host found!");
    }

    protected Connection openConnection(Properties info, String connectUrl) throws SQLException {
        return DriverManager.getConnection(connectUrl, info);
    }


    private ConnectionChecker chooseConnectionChecker(String protocol, Map<String, List<String>> queryParameters) {
        if(protocol.equals(mysqlReadClusterConnectorName)) {
            return new ReadClusterConnectionChecker(queryParameters);
        } else if(protocol.equals(galeraClusterConnectorName)) {
            return new GaleraClusterConnectionChecker();
        }
        throw new UnsupportedOperationException("Unsupported protocol: "+protocol);
    }

    private InvocationHandler createProxyWrapper(final ConnectionChecker connectionChecker, Connection actualConnection) {
        return (proxy, method, args) -> {
            if(method.getName().equals("isValid")) {
                return connectionChecker.connectionOk(actualConnection);
            } else {
                return method.invoke(actualConnection, args);
            }
        };
    }

}
