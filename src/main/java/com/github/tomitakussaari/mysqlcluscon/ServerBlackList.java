package com.github.tomitakussaari.mysqlcluscon;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class ServerBlackList {

    private static final long blackListTimeInMs = 2 * 60 * 1000;
    private final Map<String, Long> hostsAndBlackListTimes = new ConcurrentHashMap<>();

    private final Supplier<Long> nowSupplier;

    ServerBlackList() {
        this(System::currentTimeMillis);
    }

    ServerBlackList(Supplier<Long> nowSupplier) {
        this.nowSupplier = nowSupplier;
    }

    void blackList(String host) {
        hostsAndBlackListTimes.put(host, nowSupplier.get());
    }

    List<String> filterOutBlacklisted(List<String> allHosts) {
        purgeOldEntries();
        return allHosts.stream()
                .filter(host -> !hostsAndBlackListTimes.containsKey(host))
                .collect(Collectors.toList());
    }

    private void purgeOldEntries() {
        hostsAndBlackListTimes.entrySet()
                .stream()
                .filter(entry -> entry.getValue() + blackListTimeInMs < nowSupplier.get())
                .forEach(entry -> hostsAndBlackListTimes.remove(entry.getKey()));
    }
}
