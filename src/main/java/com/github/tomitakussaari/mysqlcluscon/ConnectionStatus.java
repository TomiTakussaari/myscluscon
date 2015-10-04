package com.github.tomitakussaari.mysqlcluscon;

import java.util.Optional;

public enum ConnectionStatus {

    DEAD(0), STOPPED(1), BEHIND(2), OK(3);

    public final Integer priority;
    ConnectionStatus(int i) {
        this.priority = i;
    }

    public static Optional<ConnectionStatus> from(String s) {
        try {
            return Optional.of(ConnectionStatus.valueOf(s.toUpperCase()));
        } catch(IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
