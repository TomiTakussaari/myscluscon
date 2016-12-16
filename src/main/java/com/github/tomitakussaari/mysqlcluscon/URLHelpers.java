package com.github.tomitakussaari.mysqlcluscon;

import lombok.RequiredArgsConstructor;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

class URLHelpers {

    private static final Pattern urlParsePattern = Pattern.compile("(.*)://(.*)/(.*)?");


    @RequiredArgsConstructor
    static class URLInfo {

        final String protocol;
        final List<String> servers;
        final String database;
        final Map<String, List<String>> queryParameters;
        final MysclusconDriver.DriverType driverType;

        String asJdbcConnectUrl(String server) {
            return driverType.getDriverPrefix() + "://" + server + "/" + database + toQueryParametersString(queryParameters);
        }

        @Override
        public String toString() {
            return protocol + "://" + servers + "/" + database + toQueryParametersString(queryParameters);
        }
    }

    static String toQueryParametersString(Map<String, List<String>> queryParameters) {
        if (queryParameters.isEmpty()) {
            return "";
        }
        return "?" + queryParameters.entrySet()
                .stream()
                .map(entry -> entry.getValue().stream().map(value -> entry.getKey() + "=" + value).collect(Collectors.joining("&")))
                .collect(Collectors.joining("&"));
    }

    static URLInfo parse(String jdbcUrl) throws SQLException {
        Matcher matcher = urlParsePattern.matcher(jdbcUrl);

        if (matcher.find()) {
            String protocol = matcher.group(1);
            String servers = matcher.group(2);
            String database = matcher.group(3).split("\\?")[0]; //remove queryparams
            List<String> serverList = Stream.of(servers.split(",")).map(host -> host.contains(":") ? host : host + ":3306").collect(Collectors.toList());
            return new URLInfo(protocol, serverList, database, getQueryParameters(jdbcUrl), MysclusconDriver.DriverType.fromProtocol(protocol));

        } else {
            throw new SQLException("Unable to parse jdbc url: " + jdbcUrl + " with regexp: " + urlParsePattern);
        }
    }

    static String getParameter(Map<String, List<String>> queryParameters, String parameter, String defaultValue) {
        return queryParameters.getOrDefault(parameter, emptyList()).stream().findFirst().orElse(defaultValue);
    }

    private static Map<String, List<String>> getQueryParameters(String url) throws SQLException {
        final Map<String, List<String>> queryParameters = new LinkedHashMap<>();
        final int startOfQueryParams = url.indexOf("?");
        if (startOfQueryParams < 0) {
            return queryParameters;
        }
        return parseQueryParameters(url, queryParameters, startOfQueryParams + 1);

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
            throw new SQLException("Unable to decode url using UTF-8: " + substring, e);
        }
    }


}
