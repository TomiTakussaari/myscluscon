package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConnectionStatusTest {

    @Test
    public void resolveFromString() {
        assertEquals(ConnectionStatus.OK, ConnectionStatus.from("ok").get());
        assertFalse(ConnectionStatus.from("not_found").isPresent());
    }

}