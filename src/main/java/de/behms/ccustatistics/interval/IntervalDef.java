package de.behms.ccustatistics.interval;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class IntervalDef {

    public enum Size {
        HOUR, DAY, WEEK, MONTH, YEAR
    }

    private final Size size;

    public IntervalDef(Size size) {
        this.size = size;
    }

    public Size getSize() {
        return size;
    }

    // Get current interval start
    public LocalDateTime getIntervalStart(LocalDateTime time) {
        switch (size) {
            case HOUR:
                return time.truncatedTo(HOURS);
            case DAY:
                return time.truncatedTo(DAYS);
            case WEEK:
                final DayOfWeek firstDayOfWeek = WeekFields.of(Locale.getDefault()).getFirstDayOfWeek();
                return time.truncatedTo(DAYS).with(TemporalAdjusters.previousOrSame(firstDayOfWeek));
            case MONTH:
                return time.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
            case YEAR:
                return time.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
            default:
                return null;
        }
    }

    // Get previous interval start
    public LocalDateTime getPreviousIntervalStart(LocalDateTime time) {
        switch (size) {
            case HOUR:
                return getIntervalStart(time).minusHours(1);
            case DAY:
                return getIntervalStart(time).minusDays(1);
            case WEEK:
                return getIntervalStart(time).minusDays(7);
            case MONTH:
                return getIntervalStart(time).minusMonths(1);
            case YEAR:
                return getIntervalStart(time).minusYears(1);
            default:
                return null;
        }
    }

    // Get next interval start
    public LocalDateTime getNextIntervalStart(LocalDateTime time) {
        switch (size) {
            case HOUR:
                return getIntervalStart(time).plusHours(1);
            case DAY:
                return getIntervalStart(time).plusDays(1);
            case WEEK:
                return getIntervalStart(time).plusDays(7);
            case MONTH:
                return getIntervalStart(time).plusMonths(1);
            case YEAR:
                return getIntervalStart(time).plusYears(1);
            default:
                return null;
        }
    }
}
