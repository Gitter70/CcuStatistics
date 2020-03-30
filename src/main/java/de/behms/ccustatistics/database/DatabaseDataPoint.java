package de.behms.ccustatistics.database;

import de.behms.ccustatistics.Arguments;
import de.behms.ccustatistics.interval.IntervalDef;
import de.behms.ccustatistics.DataPointProcessor;
import java.sql.SQLException;
import javax.sql.RowSet;

public class DatabaseDataPoint extends Database {

    private final String dataPointOriginalName;

    public DatabaseDataPoint(Arguments arguments) throws SQLException, ClassNotFoundException {
        super(arguments);

        // Retrieve original data point name
        RowSet rowSet = executeQuery("select table_name from data_points where interface = '" + arguments.getInterface() + "' and address = '" + arguments.getAddress() + "' and identifier = '" + arguments.getIdentifier() + "'");
        if (!rowSet.last() || rowSet.getRow() != 1) {
            throw new IllegalArgumentException("Original data point with interface '" + arguments.getInterface() + "', address '" + arguments.getAddress() + "' and identifier '" + arguments.getIdentifier() + "' not existing!");
        }

        dataPointOriginalName = rowSet.getString(1);

        // Check that original data point name begins with D_ for numeric data points
        if (!dataPointOriginalName.startsWith("D_")) {
            throw new IllegalArgumentException("Original data point is not containing numeric data!");
        }
    }

    public DataPointSource getDataPointSource(DataPointProcessor.StatisticType statisticType, IntervalDef.Size size) {
        return new DataPointSource(this, statisticType, size, dataPointOriginalName);
    }

    public DataPointTarget getDataPointTarget(DataPointProcessor.StatisticType statisticType, IntervalDef.Size size) {
        return new DataPointTarget(this, statisticType, size, dataPointOriginalName);
    }
}
