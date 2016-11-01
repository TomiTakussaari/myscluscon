package com.github.tomitakussaari.mysqlcluscon;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class URLHelpers {

    static class URLInfo {
        final String protocol;
        final List<String> servers;
        final String database;
        final Map<String, List<String>> queryParameters;

        URLInfo(String protocol, List<String> servers, String database, Map<String, List<String>> queryParameters) {
            this.protocol = protocol;
            this.servers = servers;
            this.database = database;
            this.queryParameters = queryParameters;
        }

        String asJdbcConnectUrl(String server) {
            return "jdbc:mysql://"+server+database+toQueryParametersString(queryParameters);
        }
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

    static URLInfo parse(String jdbcUrl) throws SQLException {
        //TODO horrible
        //TODO: make safer & cleaner!
        String afterProto = jdbcUrl.substring(jdbcUrl.indexOf("://")+3, jdbcUrl.length());
        String database = "/";
        if(afterProto.contains("/")) {
            database = afterProto.substring(afterProto.indexOf("/"), afterProto.contains("?") ? afterProto.indexOf("?") : afterProto.length());
            afterProto = afterProto.replaceAll(database, "");
        }
        String servers = afterProto.contains("?") ? afterProto.substring(0, afterProto.indexOf("?")): afterProto;
        List<String> serverList = Stream.of(servers.split(",")).map(host -> host.contains(":") ? host : host + ":3306").collect(Collectors.toList());

        return new URLInfo(getProtocol(jdbcUrl), serverList, database, getQueryParameters(jdbcUrl));
    }

    static String getProtocol(String jdbcUrl) {
        return jdbcUrl.substring(0, jdbcUrl.indexOf("://")); //TODO: safer & cleaner ?!
    }

    static String getParameter(Map<String, List<String>> queryParameters, String parameter, String defaultValue) {
        return queryParameters.getOrDefault(parameter, new ArrayList<>()).stream().findFirst().orElse(defaultValue);
    }

    private static Map<String, List<String>> getQueryParameters(String url) throws SQLException {
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
