package com.github.tomitakussaari.mysqlcluscon;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ServerBlackListTest {

    private Supplier<Long> nowSupplier = Mockito.mock(Supplier.class);
    private ServerBlackList serverBlackList;

    @Before
    public void before() {
        serverBlackList = new ServerBlackList(nowSupplier);
    }

    @Test
    public void filtersOutBlacklistedServers() {
        when(nowSupplier.get()).thenReturn(System.currentTimeMillis());
        serverBlackList.blackList("server1.fi:3306");
        List<String> filteredList = serverBlackList.filterOutBlacklisted(Arrays.asList("server1.fi:3306", "server1.fi:3307"));
        assertEquals(1, filteredList.size());
        assertEquals("server1.fi:3307", filteredList.get(0));
    }

    @Test
    public void returnsCurrentlyBlacklistedServers() {
        when(nowSupplier.get()).thenReturn(System.currentTimeMillis());
        assertTrue(serverBlackList.blackListed().isEmpty());
        serverBlackList.blackList("server1.fi:3306");
        assertTrue(serverBlackList.blackListed().contains("server1.fi:3306"));
    }

    @Test
    public void clearsOldEntriesFromBlackList() {
        when(nowSupplier.get()).thenReturn(100 * 60 * 1000L);
        serverBlackList.blackList("server1.fi:3306");
        when(nowSupplier.get()).thenReturn(200 * 60 * 1000L);
        List<String> filteredList = serverBlackList.filterOutBlacklisted(Arrays.asList("server1.fi:3306", "server1.fi:3307"));
        assertEquals(2, filteredList.size());

    }
}