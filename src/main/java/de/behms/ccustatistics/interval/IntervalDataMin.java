package de.behms.ccustatistics.interval;

import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.database.Event;
import java.time.LocalDateTime;

public class IntervalDataMin extends IntervalData {

    public IntervalDataMin(LocalDateTime interval, Event lastEventBeforeThisInterval) {
        super(Size.HOUR, interval, lastEventBeforeThisInterval);
    }

    public IntervalDataMin(Size size, LocalDateTime interval) {
        super(size, interval);
    }

    @Override
    protected double getHourResultValue() {
        double min = Double.MAX_VALUE;
        for (Event event : events) {
            if (event.value < min) {
                min = event.value;
            }
        }

        MyLogger.Log(3,
                new MyLogger.Item("", 0, "Resulting minimum:", -40), new MyLogger.Item("Minimum", 0, min, 10));

        return min;
    }

    @Override
    protected double getDayWeekMonthYearResultValue() {
        return getHourResultValue();
    }
}
