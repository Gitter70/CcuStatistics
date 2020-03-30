package de.behms.ccustatistics.interval;

import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.database.Event;
import java.time.Duration;
import java.time.LocalDateTime;

public class IntervalDataAvg extends IntervalData {

    public IntervalDataAvg(LocalDateTime interval, Event lastEventBeforeThisInterval) {
        super(Size.HOUR, interval, lastEventBeforeThisInterval);
    }

    public IntervalDataAvg(Size size, LocalDateTime interval) {
        super(size, interval);
    }

    @Override
    protected double getHourResultValue() {
        Event lastEvent = null;
        double avgValueXsecondsSum = 0.0;

        for (Event currentEvent : events) {
            if (lastEvent != null) {
                // Calculate average value between current and last event
                double avgValue = lastEvent.value + (currentEvent.value - lastEvent.value) / 2.0;

                // Update variable containing value * duration fragment sum to derive average value from later
                Duration durationBetweenEvents = Duration.between(lastEvent.timestamp, currentEvent.timestamp);
                double durationBetweenEventsSeconds = durationBetweenEvents.getSeconds() + durationBetweenEvents.getNano() / 1000000000.0;
                double avgValueXseconds = avgValue * durationBetweenEventsSeconds;

                avgValueXsecondsSum += avgValueXseconds;

                MyLogger.Log(3,
                        new MyLogger.Item("", 0, "Calculating average:", -40), new MyLogger.Item("Average", 0, avgValue, 10), new MyLogger.Item(", Duration", 0, durationBetweenEventsSeconds, 10),
                        new MyLogger.Item(", Average*duration", 0, avgValueXseconds, 10), new MyLogger.Item(", Average*duration sum", 0, avgValueXsecondsSum, 10),
                        new MyLogger.Item(" (Last event: Time", 0, lastEvent.timestamp, 0), new MyLogger.Item(", Value", 0, lastEvent.value, 0),
                        new MyLogger.Item(" / Current event: Time", 0, currentEvent.timestamp, 0), new MyLogger.Item(", Value", 0, currentEvent.value, 0),
                        new MyLogger.Item("", 0, ")", 0));
            }

            lastEvent = currentEvent;
        }

        Duration intervalDuration = Duration.between(getPreviousIntervalStart(interval), interval);
        double intervalDurationSeconds = intervalDuration.getSeconds() + intervalDuration.getNano() / 1000000000.0;
        double avg = avgValueXsecondsSum / intervalDurationSeconds;

        MyLogger.Log(3, new MyLogger.Item("", 0, "Resulting average:", -40), new MyLogger.Item("Average", 0, avg, 10));

        return avg;
    }

    @Override
    protected double getDayWeekMonthYearResultValue() {
        double sum = 0.0;
        for (Event event : events) {
            sum += event.value;
        }

        double avg = 0.0;
        if (!events.isEmpty()) {
            avg = sum / events.size();
        }

        MyLogger.Log(3,
                new MyLogger.Item("", 0, "Resulting average:", -40), new MyLogger.Item("Average", 0, avg, 10),
                new MyLogger.Item(" (Value sum", 0, sum, 0), new MyLogger.Item(", Number of values", 0, events.size(), 0),
                new MyLogger.Item("", 0, ")", 0)
        );

        return avg;
    }
}
