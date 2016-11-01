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
        Map<String, List<String>> queryParameters = URLHelpers.parse("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false").queryParameters;
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
        Map<String, List<String>> queryParameters = URLHelpers.parse("jdbc:mysql://server.domain.fi/database?foobar=true").queryParameters;
        assertEquals("[true]", queryParameters.get("foobar").toString());
    }

    @Test
    public void parsesMultipleQueryParametersForSameKey() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.parse("jdbc:mysql://server.domain.fi/database?foobar=bar&foobar=foo").queryParameters;
        assertTrue(queryParameters.get("foobar").contains("bar"));
        assertTrue(queryParameters.get("foobar").contains("foo"));
    }

    @Test
    public void valueLessQueryParam() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.parse("jdbc:mysql://server.domain.fi/database?foobar").queryParameters;
        assertTrue(queryParameters.containsKey("foobar"));
        assertEquals(1, queryParameters.get("foobar").size());
        assertEquals(null, queryParameters.get("foobar").get(0));
    }

    @Test
    public void parsesSingleServerFromUrl() throws SQLException {
        assertEquals("[serverOne:3306]", URLHelpers.parse("jdbc:myscluscon:mysql:read_cluster://serverOne/database").servers.toString());
    }

    @Test
    public void parsesMultipleServersWithSamePortFromUrl() throws SQLException {
        assertEquals("[serverOne:3306, serverTwo:3306, ServerThree:2134]", URLHelpers.parse("jdbc:myscluscon:mysql:read_cluster://serverOne,serverTwo,ServerThree:2134/database").servers.toString());
    }

    @Test
    public void parsesMultipleServersWithDifferentPortsFromUrl() throws SQLException {
        assertEquals("[serverOne:1234, serverTwo:3306, ServerThree:2134]", URLHelpers.parse("jdbc:myscluscon:mysql:read_cluster://serverOne:1234,serverTwo:3306,ServerThree:2134/database").servers.toString());
    }

    @Test
    public void constructsMysqlConnectUrl() throws SQLException {
        String url = "http://this.part.is.ignored/database?foobar=true&barfoo=false";
        assertEquals("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false", URLHelpers.parse(url).asJdbcConnectUrl("server.domain.fi"));
    }

    @Test
    public void constructsMysqlConnectUrlWithoutParams() throws SQLException {
        String url = "http://this.part.is.ignored/database";
        assertEquals("jdbc:mysql://server.domain.fi/database", URLHelpers.parse(url).asJdbcConnectUrl("server.domain.fi"));
    }

    @Test
    public void constructsMysqlConnectUrlWithDefinedPort() throws MalformedURLException, SQLException {
        String url = "http://this.part.is.ignored:12345/database?foobar=true&barfoo=false";
        assertEquals("jdbc:mysql://server.domain.fi:12345/database?foobar=true&barfoo=false", URLHelpers.parse(url).asJdbcConnectUrl("server.domain.fi:12345"));
    }
}