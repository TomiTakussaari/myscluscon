package com.github.tomitakussaari.mysql;

import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class URLHelpersSpec {

    @Test
    public void parsesMultipleQueryParametersCorrectly() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false");
        assertEquals("[true]", queryParameters.get("foobar").toString());
        assertEquals("[false]", queryParameters.get("barfoo").toString());
    }

    @Test
    public void parsesSingleQueryParameterCorrectly() throws SQLException {
        Map<String, List<String>> queryParameters = URLHelpers.getQueryParameters("jdbc:mysql://server.domain.fi/database?foobar=true");
        assertEquals("[true]", queryParameters.get("foobar").toString());
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
    public void constructsMysqlConnectUrl() throws MalformedURLException {
        URL url = new URL("http://this.part.is.ignored/database?foobar=true&barfoo=false");
        assertEquals("jdbc:mysql://server.domain.fi/database?foobar=true&barfoo=false", URLHelpers.constructMysqlConnectUrl(url, "server.domain.fi"));
    }

    @Test
    public void constructsMysqlConnectUrlWithDefinedPort() throws MalformedURLException {
        URL url = new URL("http://this.part.is.ignored:12345/database?foobar=true&barfoo=false");
        assertEquals("jdbc:mysql://server.domain.fi:12345/database?foobar=true&barfoo=false", URLHelpers.constructMysqlConnectUrl(url, "server.domain.fi"));
    }


}