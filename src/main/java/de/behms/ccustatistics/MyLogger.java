package de.behms.ccustatistics;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.LogManager;

public class MyLogger {

    private static MyLogger myLogger;

    private final java.util.logging.Logger logger;

    public static class Item {

        private String string = new String();

        public Item(String value) {
            string = value;
        }

        public Item(String attribute, int attributePadding, String value, int valuePadding) {
            if (addString(attribute, attributePadding)) {
                string += " = ";
            }

            addString(value, valuePadding);
        }

        public Item(String attribute, int attributePadding, int value, int valuePadding) {
            this(attribute, attributePadding, Integer.toString(value), valuePadding);
        }

        public Item(String attribute, int attributePadding, Double value, int valuePadding) {
            this(attribute, attributePadding, String.format("%.3f", value), valuePadding);
        }

        public Item(String attribute, int attributePadding, LocalDateTime value, int valuePadding) {
            this(attribute, attributePadding, GetTimeString(value), valuePadding);
        }

        private boolean addString(String input, int padding) {
            if (input.isEmpty()) {
                return false;
            }

            if (padding == 0) {
                string += input;
            } else {
                string += String.format("%" + padding + "s", input);
            }

            return true;
        }
    }

    public static void Log(int indentLevel, String message) {
        Log(indentLevel, message, new Item[0]);
    }

    public static void Log(int indentLevel, Item... items) {
        Log(indentLevel, "", items);
    }

    public static void Log(int indentLevel, String header, Item... items) {
        String message;
        if (indentLevel > 0) {
            message = String.format("%" + indentLevel * 4 + "s", "") + header;
        } else {
            message = header;
        }

        for (Item item : items) {
            message += item.string;
        }

        getMyLogger().logger.log(Level.INFO, message);
    }

    public static void LogException(String message, Throwable throwable) {
        getMyLogger().logger.log(Level.SEVERE, message, throwable);
    }

    public static String GetTimeString(LocalDateTime value) {
        return value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    private static MyLogger getMyLogger() {
        if (myLogger == null) {
            myLogger = new MyLogger();
        }

        return myLogger;
    }

    private MyLogger() {
        // Read logging properties
        String logFile = System.getProperty("java.util.logging.config.file");

        // Get logging.properties from resource if property is not set
        if (logFile == null) {
            try {
                LogManager.getLogManager().readConfiguration(MyLogger.class.getClassLoader().getResourceAsStream("logging.properties"));
            } catch (IOException e) {
                e.printStackTrace(System.out);
                System.exit(0);
            }
        }

        // Get logger
        logger = java.util.logging.Logger.getLogger(MyLogger.class.getName());
    }
}
