package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;

import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

public class URLHelpersTest {

    @Test
    public void toQueryParametersStringWithMultipleValues() {
        Map<String, List<String>> queryParams = new TreeMap<>();
        queryParams.put("foo", Arrays.asList("1", "2"));
        queryParams.put("bar", Arrays.asList("3", "4"));
        queryParams.put("foobar", Collections.singletonList("5"));
        assertEquals("?bar=3&bar=4&foo=1&foo=2&foobar=5", URLHelpers.toQueryParametersString(queryParams));
    }

    @Test
    public void toQueryParametersStringWithNoQueryParameters() {
        Map<String, List<String>> queryParams = new TreeMap<>();
        assertEquals("", URLHelpers.toQueryParametersString(queryParams));
    }

    @Test
    public void parsesMultipleQueryParametersCorrectly() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false");
        assertEquals("[true]", queryParameters.get("foobar").toString());
        assertEquals("[false]", queryParameters.get("barfoo").toString());
    }

    @Test
    public void parsesProtocol() throws SQLException {
        String protocol = URLHelpers.getProtocol("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false");
        assertEquals("jdbc:mysql", protocol);
    }

    @Test
    public void parsesSingleQueryParameterCorrectly() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters("jdbc:mysql://server.domain.fi/database?foobar=true");
        assertEquals("[true]", queryParameters.get("foobar").toString());
    }

    @Test
    public void parsesMultipleQueryParametersForSameKey() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters("jdbc:mysql://server.domain.fi/database?foobar=bar&foobar=foo");
        assertTrue(queryParameters.get("foobar").contains("bar"));
        assertTrue(queryParameters.get("foobar").contains("foo"));
    }

    @Test
    public void valueLessQueryParam() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters("jdbc:mysql://server.domain.fi/database?foobar");
        assertTrue(queryParameters.containsKey("foobar"));
        assertEquals(1, queryParameters.get("foobar").size());
        assertEquals(null, queryParameters.get("foobar").get(0));
    }

    @Test
    public void parsesSingleHostFromUrl() throws MalformedURLException {
        assertEquals("[serverOne]", URLHelpers.getHosts("jdbc:myscluscon:mysql:read_cluster://serverOne/database").toString());
    }

    @Test
    public void parsesMultipleHostsWithPortsFromUrl() throws MalformedURLException {
        assertEquals("[serverOne, serverTwo, ServerThree]", URLHelpers.getHosts("jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,ServerThree:2134/database").toString());
    }

    @Test
    public void constructsMysqlConnectUrl() throws MalformedURLException, SQLException {
        String url = "http://this.part.is.ignored/database?foobar=true&barfoo=false";
        assertEquals("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false", URLHelpers.constructMysqlConnectUrl("server.domain.fi", url, URLHelpers.getQueryParameters(url)));
    }

    @Test
    public void constructsMysqlConnectUrlWithoutParams() throws MalformedURLException, SQLException {
        String url = "http://this.part.is.ignored/database";
        assertEquals("jdbc:mysql://server.domain.fi/database", URLHelpers.constructMysqlConnectUrl("server.domain.fi", url, URLHelpers.getQueryParameters(url)));
    }

    @Test
    public void constructsMysqlConnectUrlWithDefinedPort() throws MalformedURLException, SQLException {
        String url = "http://this.part.is.ignored:12345/database?foobar=true&barfoo=false";
        assertEquals("jdbc:mysql://server.domain.fi:12345/database?foobar=true&barfoo=false", URLHelpers.constructMysqlConnectUrl("server.domain.fi", url, URLHelpers.getQueryParameters(url)));
    }

    @Test
    public void invalidUrl() {
        try {
            URLHelpers.createURL("foobar");
            fail("Should have failed");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof MalformedURLException);
        }
    }


}