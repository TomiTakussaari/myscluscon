package com.github.tomitakussaari.mysqlcluscon;

import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RequiredArgsConstructor
class ServerBlackList {

    private static final long defaultBlackListTimeInMs = 2 * 60 * 1000;
    private final Map<String, Long> serversAndBlackListTimes = new ConcurrentHashMap<>();

    private final Supplier<Long> nowSupplier;
    private final long blackListTimeInMs;

    ServerBlackList() {
        this(System::currentTimeMillis, defaultBlackListTimeInMs);
    }

    void blackList(String server) {
        serversAndBlackListTimes.put(server, nowSupplier.get());
    }

    List<String> withoutBlackListed(List<String> allServers) {
        purgeOldEntries();
        return allServers.stream()
                .filter(server -> !serversAndBlackListTimes.containsKey(server))
                .collect(Collectors.toList());
    }

    private void purgeOldEntries() {
        Set<Map.Entry<String, Long>> blackListEntries = serversAndBlackListTimes.entrySet();
        blackListEntries.stream()
                .filter(entry -> entry.getValue() + blackListTimeInMs < nowSupplier.get())
                .forEach(blackListEntries::remove);
    }

    Set<String> blackListed() {
        purgeOldEntries();
        return serversAndBlackListTimes.keySet();
    }
}
