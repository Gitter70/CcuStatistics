package de.behms.ccustatistics.database;

import java.time.LocalDateTime;

public class Event {

    public LocalDateTime timestamp;
    public double value;

    public Event(LocalDateTime timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }
}
