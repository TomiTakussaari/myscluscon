package com.github.tomitakussaari.mysqlcluscon;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.*;

class URLHelpers {

    public static String constructMysqlConnectUrl(URL originalUrl, String host) {
        final String protocol = "jdbc:mysql";
        final String port = originalUrl.getPort() != -1 ? ":"+originalUrl.getPort() : "";
        final String database = originalUrl.getPath();
        final String queryParams = Optional.ofNullable(originalUrl.getQuery()).map(query -> "?"+query).orElse("");
        return protocol+"://"+host+port+database+queryParams;
    }

    public static List<String> getHosts(String jdbcUrl) {
        URL url = createConvertedUrl(jdbcUrl);
        return Arrays.asList(url.getHost().split(","));
    }

    public static String getProtocol(String jdbcUrl) {
        return jdbcUrl.substring(0, jdbcUrl.indexOf("://"));
    }

    public static URL createConvertedUrl(String jdbcUrl) {
        try {
            //Hacky but it feels simpler than alternatives, as Java URL only supports certain protocols, so protocol is "changed" here to make URL work..
            return new URL(jdbcUrl.replace(MysclusconDriver.galeraClusterConnectorName, "http").replace(MysclusconDriver.mysqlReadClusterConnectorName, "http"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, List<String>> getQueryParameters(String url) throws SQLException {
        final Map<String, List<String>> queryParameters = new LinkedHashMap<>();
        final int startOfQueryParams = url.indexOf("?");
        if(startOfQueryParams < 0) {
            return queryParameters;
        }
        return parseQueryParameters(url, queryParameters, startOfQueryParams+1);

    }

    private static Map<String, List<String>> parseQueryParameters(String url, Map<String, List<String>> queryParameters, int startOfQueryParams) throws SQLException {
        final String[] pairs = url.substring(startOfQueryParams).split("&");
        try {
            for (String pair : pairs) {
                final int idx = pair.indexOf("=");
                final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
                if (!queryParameters.containsKey(key)) {
                    queryParameters.put(key, new LinkedList<>());
                }
                final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
                queryParameters.get(key).add(value);
            }
            return queryParameters;
        } catch (UnsupportedEncodingException e) {
            throw new SQLException(e);
        }
    }


}
