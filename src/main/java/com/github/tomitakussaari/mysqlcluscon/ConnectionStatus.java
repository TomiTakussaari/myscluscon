package com.github.tomitakussaari.mysqlcluscon;

public enum ConnectionStatus {

    DEAD(0), STOPPED(1), BEHIND(2), OK(3);

    public final Integer priority;
    ConnectionStatus(int i) {
        this.priority = i;
    }

    public boolean usable() {
        return this != DEAD;
    }

}
