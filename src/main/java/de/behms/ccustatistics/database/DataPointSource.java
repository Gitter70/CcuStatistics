package de.behms.ccustatistics.database;

import de.behms.ccustatistics.interval.IntervalDef;
import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.DataPointProcessor;

public class DataPointSource extends DataPoint {

    protected DataPointSource(Database database, DataPointProcessor.StatisticType statisticType, IntervalDef.Size size, String dataPointOriginalName) {
        super(database);

        // Retrieve source data point name
        switch (size) {
            case HOUR:
                // Use original data point as source data point
                name = dataPointOriginalName;
                break;
            case DAY:
                // Use hour target data point as new source data point (source data point must be of same statistic type as target data point)
                name = "D_STATISTIC_" + dataPointOriginalName + "_" + statisticType + "_HOUR";
                break;
            default:
                // For week use day target data point as new source data point (source data point must be of same statistic type as target data point)
                // For month and year keep day as source data point as a week may span over two months
                name = "D_STATISTIC_" + dataPointOriginalName + "_" + statisticType + "_DAY";
        }

        MyLogger.Log(1, new MyLogger.Item("Data point source", -20, name, 0));
    }
}
