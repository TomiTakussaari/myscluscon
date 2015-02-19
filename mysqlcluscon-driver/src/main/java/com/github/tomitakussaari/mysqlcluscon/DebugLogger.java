package com.github.tomitakussaari.mysqlcluscon;

public class DebugLogger {

    final static boolean loggingEnabled = System.getProperty(DebugLogger.class.getCanonicalName().concat(".debug"), "false").equals("true");

    public static void debug(String message) {
        if(loggingEnabled) {
            System.out.println(message);
        }
    }
}
