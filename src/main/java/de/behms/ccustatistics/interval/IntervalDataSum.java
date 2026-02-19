package de.behms.ccustatistics.interval;

import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.database.Event;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.ListIterator;

public class IntervalDataSum extends IntervalData {

    public IntervalDataSum(LocalDateTime interval, Event lastEventBeforeThisInterval) {
        super(Size.HOUR, interval, lastEventBeforeThisInterval);
    }

    public IntervalDataSum(Size size, LocalDateTime interval) {
        super(size, interval);
    }

    public static LinkedList<Event> preProcessEvents(LinkedList<Event> events, Double maxJump) {
        MyLogger.Log(0, "Preprocessing rising only hour interval events");

        events = filterNonSteadyIntervals(events, maxJump);

        if (!events.isEmpty()) {
            normalizeValues(events, maxJump);
        }

        return events;
    }

    private static LinkedList<Event> filterNonSteadyIntervals(LinkedList<Event> events, Double maxJump) {
        MyLogger.Log(1, "Filtering non steady intervals");

        LinkedList<Event> resultEvents = new LinkedList<>();
        LinkedList<Event> hourIntervalEvents = new LinkedList<>();

        LocalDateTime predecessorEventInterval = null;
        boolean steadyInterval = false;
        int steadyIntervalCount = 0;

        IntervalDef intervalDef = new IntervalDef(Size.HOUR);

        for (ListIterator<Event> it = events.listIterator(); it.hasNext();) {
            Event currentEvent = it.next();

            LocalDateTime currentEventInterval = intervalDef.getIntervalStart(currentEvent.timestamp);

            if (predecessorEventInterval == null || !currentEventInterval.equals(predecessorEventInterval)) {
                if (steadyInterval) {
                    steadyIntervalCount++;
                    resultEvents.addAll(hourIntervalEvents);
                } else {
                    steadyIntervalCount = 0;
                }

                MyLogger.Log(2, "Processing hour interval beginning at " + MyLogger.GetTimeString(currentEventInterval) + " (Steady intervals = " + steadyIntervalCount + ")");
                steadyInterval = true;
                hourIntervalEvents.clear();
            }

            if (steadyInterval) {
                if (!hourIntervalEvents.isEmpty()) {
                    if (currentEvent.value < hourIntervalEvents.getLast().value) {
                        logEvent("Reset or time change, skip interval", currentEvent);
                        steadyInterval = false;
                    } else if (maxJump != null && currentEvent.value > hourIntervalEvents.getLast().value + maxJump) {
                        logEvent("Jump detected, skip interval", currentEvent);
                        steadyInterval = false;
                    }
                }
            }

            if (steadyInterval) {
                logEvent("Processing event", currentEvent);
                hourIntervalEvents.add(currentEvent);
            } else {
                logEvent("Skipping event", currentEvent);
            }

            predecessorEventInterval = currentEventInterval;
        }

        if (steadyIntervalCount < 2) {
            MyLogger.Log(1, "Skipping data as last two intervals are not both steady");
            resultEvents.clear();
        }

        return resultEvents;
    }

    private static void normalizeValues(LinkedList<Event> events, Double maxJump) {
        MyLogger.Log(1, "Normalizing values taking resets and value jumps into account");

        Event predecessorEvent = null;
        double offset = 0.0;

        for (ListIterator<Event> it = events.listIterator(); it.hasNext();) {
            Event currentEvent = it.next();

            String header = "Adding event";
            double offsetSummand = 0.0;

            if (predecessorEvent != null) {
                if (currentEvent.value < predecessorEvent.value) {
                    header = "Reset detected, adapting value offset";
                    offsetSummand = predecessorEvent.value;
                    offset += offsetSummand;
                } else if (maxJump != null && currentEvent.value > predecessorEvent.value + maxJump) {
                    header = "Jump detected, adapting value offset";
                    offsetSummand = predecessorEvent.value - currentEvent.value;
                    offset += offsetSummand;
                }
            }

            predecessorEvent = new Event(currentEvent.timestamp, currentEvent.value);

            double currentEventNewValue = currentEvent.value + offset;

            MyLogger.Log(3,
                    new MyLogger.Item("", 0, header, -40), new MyLogger.Item("Time", 0, currentEvent.timestamp, 0), new MyLogger.Item(", Value", 0, currentEventNewValue, 10),
                    new MyLogger.Item(" (Offset summand", 0, offsetSummand, 0), new MyLogger.Item(", Offset", 0, offset, 0), new MyLogger.Item(", Original value", 0, currentEvent.value, 0),
                    new MyLogger.Item("", 0, ")", 0));

            currentEvent.value = currentEventNewValue;
        }
    }

    @Override
    protected double getHourResultValue() {
        MyLogger.Log(2, "Calculating sum from difference values");

        double sum = 0.0;

        Event lastEvent = null;
        for (Event currentEvent : events) {
            if (lastEvent != null) {
                double summand = currentEvent.value - lastEvent.value;
                sum += summand;

                MyLogger.Log(3,
                        new MyLogger.Item("", 0, "Calculating sum:", -40), new MyLogger.Item("Summand", 0, summand, 10), new MyLogger.Item(", Sum", 0, sum, 10),
                        new MyLogger.Item(" (Last event: Time", 0, lastEvent.timestamp, 0), new MyLogger.Item(", Value", 0, lastEvent.value, 0),
                        new MyLogger.Item(" / Current event: Time", 0, currentEvent.timestamp, 0), new MyLogger.Item(", Value", 0, currentEvent.value, 0),
                        new MyLogger.Item("", 0, ")", 0)
                );
            }

            lastEvent = currentEvent;
        }

        return sum;
    }

    @Override
    protected double getDayWeekMonthYearResultValue() {
        double sum = 0.0;

        for (Event event : events) {
            sum += event.value;

            MyLogger.Log(3,
                    new MyLogger.Item("", 0, "Calculating sum:", -40), new MyLogger.Item("Sum", 0, sum, 10),
                    new MyLogger.Item(" (Event: Time", 0, event.timestamp, 0), new MyLogger.Item(", Value", 0, event.value, 0),
                    new MyLogger.Item("", 0, ")", 0)
            );
        }

        return sum;
    }
}
