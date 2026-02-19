package de.behms.ccustatistics.interval;

import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.database.DataPointTarget;
import de.behms.ccustatistics.database.Event;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;

public abstract class IntervalData extends IntervalDef {

    protected final LocalDateTime interval;
    protected final Event lastEventBeforeThisInterval;

    protected LinkedList<Event> events;

    public IntervalData(Size size, LocalDateTime interval, Event lastEventBeforeThisInterval) {
        super(size);

        // Statistic interval that data is processed for
        this.interval = interval;

        // Last event before this interval
        this.lastEventBeforeThisInterval = lastEventBeforeThisInterval;

        // All events of the current interval
        this.events = new LinkedList<>();

        MyLogger.Log(1, "Creating interval beginning at " + MyLogger.GetTimeString(interval));
        MyLogger.Log(2, "Retrieving DB events");
    }

    public IntervalData(Size size, LocalDateTime interval) {
        this(size, interval, null);
    }

    public void addEvent(LocalDateTime ts, double value) {
        events.add(new Event(ts, value));
        logEvent("DB event", events.getLast());
    }

    public void generateHourStatisticData(DataPointTarget dataPointTarget, Event nextEventAfterThisInterval) throws SQLException {
        // Add transitional start/end values
        addIntervalTransitionalEvents(nextEventAfterThisInterval);

        // Get result value to be committed
        Event resultEvent = new Event(interval, getHourResultValue());

        // Commit event to database
        CommitEvent(dataPointTarget, resultEvent);
    }

    protected abstract double getHourResultValue();

    public void generateOtherStatisticData(DataPointTarget dataPointTarget) throws SQLException {
        // Get result value to be committed
        Event resultEvent = new Event(interval, getDayWeekMonthYearResultValue());

        // Commit event to database
        CommitEvent(dataPointTarget, resultEvent);
    }

    protected abstract double getDayWeekMonthYearResultValue();

    private void CommitEvent(DataPointTarget dataPointTarget, Event resultEvent) throws SQLException {
        MyLogger.Log(2, "Creating statistic entry");
        logEvent("Result event", resultEvent);

        // Insert row into table
        dataPointTarget.insertRow(resultEvent);
    }

    static protected void logEvent(String header, Event event) {
        MyLogger.Log(3, new MyLogger.Item("", 0, header + ":", -40), new MyLogger.Item("Time", 0, event.timestamp, 0), new MyLogger.Item(", Value", 0, event.value, 10));
    }

    private void addIntervalTransitionalEvents(Event nextEventAfterThisInterval) {
        MyLogger.Log(2, "Adding transitional hour start and end event");

        if (events.isEmpty() || !events.getFirst().timestamp.equals(interval)) {
            // Calculate value gradient from last event before this interval to first event following interval start
            Event firstEventAfterIntervalStart = events.isEmpty() ? nextEventAfterThisInterval : events.getFirst();
            Event intervalTransitionStartEvent = calculateIntervalTransitionEvent(lastEventBeforeThisInterval, firstEventAfterIntervalStart, interval);
            events.addFirst(intervalTransitionStartEvent);
            MyLogger.Log(3,
                    new MyLogger.Item("", 0, "Interval transition start event:", -40), new MyLogger.Item("Time", 0, intervalTransitionStartEvent.timestamp, 0), new MyLogger.Item(", Value", 0, intervalTransitionStartEvent.value, 10),
                    new MyLogger.Item(" (Last event before this interval: Time", 0, lastEventBeforeThisInterval.timestamp, 0), new MyLogger.Item(", Value", 0, lastEventBeforeThisInterval.value, 0),
                    new MyLogger.Item(" / First event after interval start: Time", 0, firstEventAfterIntervalStart.timestamp, 0), new MyLogger.Item(", Value", 0, firstEventAfterIntervalStart.value, 0),
                    new MyLogger.Item("", 0, ")", 0)
            );
        }

        if (!events.getLast().timestamp.equals(getNextIntervalStart(interval))) {
            // Calculate value gradient from last event before end of this interval to first event following interval end
            Event lastEventBeforeIntervalEnd = events.isEmpty() ? lastEventBeforeThisInterval : events.getLast();
            Event intervalTransitionEndEvent = calculateIntervalTransitionEvent(lastEventBeforeIntervalEnd, nextEventAfterThisInterval, getNextIntervalStart(interval));
            events.add(intervalTransitionEndEvent);
            MyLogger.Log(3,
                    new MyLogger.Item("", 0, "Interval transition end event:", -40), new MyLogger.Item("Time", 0, intervalTransitionEndEvent.timestamp, 0), new MyLogger.Item(", Value", 0, intervalTransitionEndEvent.value, 10),
                    new MyLogger.Item(" (Last event before interval end: Time", 0, lastEventBeforeIntervalEnd.timestamp, 0), new MyLogger.Item(", Value", 0, lastEventBeforeIntervalEnd.value, 0),
                    new MyLogger.Item(" / Next event after this interval: Time", 0, nextEventAfterThisInterval.timestamp, 0), new MyLogger.Item(", Value", 0, nextEventAfterThisInterval.value, 0),
                    new MyLogger.Item("", 0, ")", 0)
            );
        }
    }

    private Event calculateIntervalTransitionEvent(Event lastEventBeforeTransition, Event firstEventAfterTransition, LocalDateTime intervalTransitionTimestamp) {
        // Calculate value gradient
        Duration durationBetweenEvents = Duration.between(lastEventBeforeTransition.timestamp, firstEventAfterTransition.timestamp);
        double durationBetweenEventsSeconds = durationBetweenEvents.getSeconds() + durationBetweenEvents.getNano() / 1000000000.0;

        double valueGradient;
        if (durationBetweenEventsSeconds == 0.0) {
            valueGradient = 0.0;
        } else {
            valueGradient = (firstEventAfterTransition.value - lastEventBeforeTransition.value) / durationBetweenEventsSeconds;
        }

        Duration durationUntilTransition = Duration.between(lastEventBeforeTransition.timestamp, intervalTransitionTimestamp);
        double durationUntilTransitionSeconds = durationUntilTransition.getSeconds() + durationUntilTransition.getNano() / 1000000000.0;

        double transitionValue = lastEventBeforeTransition.value + valueGradient * durationUntilTransitionSeconds;
        return new Event(intervalTransitionTimestamp, transitionValue);
    }
}
