package de.behms.ccustatistics;

import de.behms.ccustatistics.interval.IntervalDef;
import de.behms.ccustatistics.database.DataPointSource;
import de.behms.ccustatistics.database.DataPointTarget;
import de.behms.ccustatistics.database.DatabaseDataPoint;
import de.behms.ccustatistics.database.Event;
import de.behms.ccustatistics.database.TimeSeries;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class DataPointProcessor {

    private final Arguments arguments;
    private final DatabaseDataPoint databaseDataPoint;

    public enum StatisticType {
        AVG, MIN, MAX, SUM
    };
    private final StatisticType statisticType;

    public DataPointProcessor(Arguments arguments, DatabaseDataPoint databaseDataPoint, StatisticType statisticType) {
        this.arguments = arguments;
        this.databaseDataPoint = databaseDataPoint;
        this.statisticType = statisticType;
    }

    // Process data
    public void processDataPoint() throws SQLException {
        MyLogger.Log(0, "########## Processing statistic type " + statisticType + " ##########");

        // Process interval sizes
        try {
            for (IntervalDef.Size size : IntervalDef.Size.values()) {
                processInterval(new IntervalDef(size));
            }
        } catch (NoSuchElementException ex) {
            // Thrown if there is no data available
            MyLogger.Log(0, ex.getMessage());
        }
    }

    // Process data for given interval size
    private void processInterval(IntervalDef intervalDef) throws SQLException {
        MyLogger.Log(0, "##### Processing " + intervalDef.getSize() + " intervals #####");
        MyLogger.Log(0, "Retrieving data point names");

        // Retrieve source data point name
        DataPointSource dataPointSource = databaseDataPoint.getDataPointSource(statisticType, intervalDef.getSize());

        // Create or delete target data point and entry in DATA_POINTS
        DataPointTarget dataPointTarget = databaseDataPoint.getDataPointTarget(statisticType, intervalDef.getSize());
        if (arguments.getDelete()) {
            dataPointTarget.delete();
            return;
        } else {
            dataPointTarget.prepare(arguments.getUnit());
        }

        // Define time ranges
        TimeDef timeDef = new TimeDef(statisticType, intervalDef, dataPointSource, dataPointTarget);

        // Process source DB data
        MyLogger.Log(0, "Retrieving DB entries");
        TimeSeries timeSeries = dataPointSource.getTimeSeries(timeDef.retrieveStartTime, null);

        // Create event list
        LinkedList<Event> events = new LinkedList<>();
        while (true) {
            Event event = timeSeries.next();

            if (event == null) {
                break;
            }

            events.add(event);
        }

        // Create event processor
        EventProcessor eventProcessor = new EventProcessor(dataPointTarget, statisticType, intervalDef, timeDef, arguments.getFactor(), arguments.getFilter());

        // Process events
        eventProcessor.processEvents(events);
    }
}
