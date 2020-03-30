package de.behms.ccustatistics.interval;

import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.database.Event;
import java.time.LocalDateTime;

public class IntervalDataMax extends IntervalData {

    public IntervalDataMax(LocalDateTime interval, Event lastEventBeforeThisInterval) {
        super(Size.HOUR, interval, lastEventBeforeThisInterval);
    }

    public IntervalDataMax(Size size, LocalDateTime interval) {
        super(size, interval);
    }

    @Override
    protected double getHourResultValue() {
        double max = Double.MIN_VALUE;
        for (Event event : events) {
            if (event.value > max) {
                max = event.value;
            }
        }

        MyLogger.Log(3,
                new MyLogger.Item("", 0, "Resulting maximum:", -40), new MyLogger.Item("Maximum", 0, max, 10));

        return max;
    }

    @Override
    protected double getDayWeekMonthYearResultValue() {
        return getHourResultValue();
    }
}
