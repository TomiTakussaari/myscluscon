package com.github.tomitakussaari.mysqlcluscon;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.*;

class URLHelpers {

    static String constructMysqlConnectUrl(URL originalUrl, String host) {
        final String protocol = "jdbc:mysql";
        final String port = originalUrl.getPort() != -1 ? ":"+originalUrl.getPort() : "";
        final String database = originalUrl.getPath();
        final String queryParams = Optional.ofNullable(originalUrl.getQuery()).map(query -> "?"+query).orElse("");
        return protocol+"://"+host+port+database+queryParams;
    }

    static List<String> getHosts(String jdbcUrl) {
        URL url = createConvertedUrl(jdbcUrl);
        return Arrays.asList(url.getHost().split(","));
    }

    static String getProtocol(String jdbcUrl) {
        return jdbcUrl.substring(0, jdbcUrl.indexOf("://"));
    }

    static URL createConvertedUrl(String jdbcUrl) {
        try {
            //Hacky but it feels simpler than alternatives, as Java URL only supports certain protocols, so protocol is "changed" here to make URL work..
            return new URL(jdbcUrl.replace(MysclusconDriver.galeraClusterConnectorName, "http").replace(MysclusconDriver.mysqlReadClusterConnectorName, "http"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, List<String>> getQueryParameters(String url) throws SQLException {
        final Map<String, List<String>> queryParameters = new LinkedHashMap<>();
        final int startOfQueryParams = url.indexOf("?");
        if(startOfQueryParams < 0) {
            return queryParameters;
        }
        return parseQueryParameters(url, queryParameters, startOfQueryParams+1);

    }

    private static Map<String, List<String>> parseQueryParameters(String url, Map<String, List<String>> queryParameters, int startOfQueryParams) throws SQLException {
        final String[] pairs = url.substring(startOfQueryParams).split("&");
        for (String pair : pairs) {
            final int idx = pair.indexOf("=");
            final String key = idx > 0 ? decode(pair.substring(0, idx)) : pair;
            List<String> values = queryParameters.computeIfAbsent(key, (k) -> new LinkedList<>());
            final String value = idx > 0 && pair.length() > idx + 1 ? decode(pair.substring(idx + 1)) : null;
            values.add(value);
        }
        return queryParameters;
    }

    private static String decode(String substring) throws SQLException {
        try {
            return URLDecoder.decode(substring, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("Error urldecoding: "+substring, e);
        }
    }


}
