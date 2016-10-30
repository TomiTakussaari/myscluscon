package com.github.tomitakussaari.mysqlcluscon;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class ServerBlackList {

    private static final long blackListTimeInMs = 2 * 60 * 1000;
    private final Map<String, Long> serversAndBlackListTimes = new ConcurrentHashMap<>();

    private final Supplier<Long> nowSupplier;

    ServerBlackList() {
        this(System::currentTimeMillis);
    }

    ServerBlackList(Supplier<Long> nowSupplier) {
        this.nowSupplier = nowSupplier;
    }

    void blackList(String server) {
        serversAndBlackListTimes.put(server, nowSupplier.get());
    }

    List<String> filterOutBlacklisted(List<String> allServers) {
        purgeOldEntries();
        return allServers.stream()
                .filter(server -> !serversAndBlackListTimes.containsKey(server))
                .collect(Collectors.toList());
    }

    private void purgeOldEntries() {
        serversAndBlackListTimes.entrySet()
                .stream()
                .filter(entry -> entry.getValue() + blackListTimeInMs < nowSupplier.get())
                .forEach(entry -> serversAndBlackListTimes.remove(entry.getKey()));
    }
}
