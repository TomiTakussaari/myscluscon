package com.github.tomitakussaari.mysqlcluscon;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

class URLHelpers {

    static String constructMysqlConnectUrl(String host, String jdbcUrl, Map<String, List<String>> queryParameters) {
        final String protocol = "jdbc:mysql";
        final URL originalUrl = createURL(jdbcUrl);
        final String port = originalUrl.getPort() != -1 ? ":"+originalUrl.getPort() : "";
        final String database = originalUrl.getPath();
        return protocol+"://"+host+port+database+toQueryParametersString(queryParameters);
    }

    static String toQueryParametersString(Map<String, List<String>> queryParameters) {
        if(queryParameters.isEmpty()) {
            return "";
        }
        return "?"+queryParameters.entrySet()
                .stream()
                .map(entry -> entry.getValue().stream().map(value -> entry.getKey()+"="+value).collect(Collectors.joining("&")))
                .collect(Collectors.joining("&"));
    }

    static List<String> getHosts(String jdbcUrl) {
        URL url = createURL(jdbcUrl);
        return Arrays.asList(url.getHost().split(","));
    }

    static String getProtocol(String jdbcUrl) {
        return jdbcUrl.substring(0, jdbcUrl.indexOf("://"));
    }

    static URL createURL(String jdbcUrl) {
        try {
            //hackiness warning:
            //Java URL by default supports only certain protocols, so protocol is "changed" here to make URL work.
            //we could implement our own URLStreamHandler, but that would then be much more code...
            return new URL(jdbcUrl.replace(MysclusconDriver.galeraClusterConnectorName, "http")
                    .replace(MysclusconDriver.mysqlReadClusterConnectorName, "http"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    static String getParameter(Map<String, List<String>> queryParameters, String parameter, String defaultValue) {
        return queryParameters.getOrDefault(parameter, new ArrayList<>()).stream().findFirst().orElse(defaultValue);
    }

    static Map<String, List<String>> getQueryParameters(String url) throws SQLException {
        final Map<String, List<String>> queryParameters = new LinkedHashMap<>();
        final int startOfQueryParams = url.indexOf("?");
        if(startOfQueryParams < 0) {
            return queryParameters;
        }
        return parseQueryParameters(url, queryParameters, startOfQueryParams+1);

    }

    private static Map<String, List<String>> parseQueryParameters(String url,
                                                                  Map<String, List<String>> queryParameters,
                                                                  int startOfQueryParams) throws SQLException {
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
            throw new SQLException("Unable to decode url using UTF-8: "+substring, e);
        }
    }


}
