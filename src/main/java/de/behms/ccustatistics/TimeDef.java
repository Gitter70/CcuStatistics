package de.behms.ccustatistics;

import de.behms.ccustatistics.interval.IntervalDef;
import de.behms.ccustatistics.database.DataPointSource;
import de.behms.ccustatistics.database.DataPointTarget;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.NoSuchElementException;

public class TimeDef {

    public LocalDateTime sourceMaxTime;
    public final LocalDateTime targetStartTime;
    public final LocalDateTime retrieveStartTime;

    public TimeDef(DataPointProcessor.StatisticType statisticType, IntervalDef intervalDef, DataPointSource dataPointSource, DataPointTarget dataPointTarget) throws SQLException, NoSuchElementException {
        MyLogger.Log(0, "Calculating time ranges");

        // Get source minimum and maximum times
        LocalDateTime sourceMinTime;

        // Get source minimum time (with full data available)
        sourceMinTime = dataPointSource.getMinTimestamp(null);
        if (sourceMinTime == null) {
            throw new NoSuchElementException("Source data point does not contain data");
        }
        sourceMinTime = intervalDef.getNextIntervalStart(sourceMinTime);

        // Get source maximum time (with full data available)
        sourceMaxTime = intervalDef.getPreviousIntervalStart(dataPointSource.getMaxTimestamp(null));

        if (statisticType == DataPointProcessor.StatisticType.SUM && intervalDef.getSize() == IntervalDef.Size.HOUR) {
            // Leave out last interval in case of rising only hour interval as
            // - This run shall end with a steady interval
            // - Next run shall start with a steady interval
            // IntervalDataDiffSum.preProcessEvents() will check that last two intervals are steady
            sourceMaxTime = intervalDef.getPreviousIntervalStart(sourceMaxTime);
        }

        MyLogger.Log(1, new MyLogger.Item("Source min time", -20, sourceMinTime, 0));
        MyLogger.Log(1, new MyLogger.Item("Source max time", -20, sourceMaxTime, 0));

        // Get target start time
        LocalDateTime time = dataPointTarget.getMaxTimestamp(null);
        if (time != null) {
            time = intervalDef.getNextIntervalStart(time);
        } else {
            time = LocalDateTime.parse("2000-01-01T00:00:00.000", DateTimeFormatter.ISO_DATE_TIME);
        }

        if (time.isBefore(sourceMinTime)) {
            targetStartTime = sourceMinTime;
        } else {
            targetStartTime = time;
        }

        MyLogger.Log(1, new MyLogger.Item("Target start time", -20, targetStartTime, 0));

        if (sourceMaxTime.isBefore(targetStartTime)) {
            throw new NoSuchElementException("No data for processing available at the moment");
        }

        // Retrieve data from target start time minus at least one interval to get start value
        retrieveStartTime = intervalDef.getIntervalStart(dataPointSource.getMaxTimestamp(targetStartTime));

        MyLogger.Log(1, new MyLogger.Item("Retrieve start time", -20, retrieveStartTime, 0));
    }
}
