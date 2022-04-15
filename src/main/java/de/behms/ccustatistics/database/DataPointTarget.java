package de.behms.ccustatistics.database;

import de.behms.ccustatistics.interval.IntervalDef;
import de.behms.ccustatistics.MyLogger;
import de.behms.ccustatistics.DataPointProcessor;
import java.sql.SQLException;
import javax.sql.RowSet;

public class DataPointTarget extends DataPoint {

    private final DataPointProcessor.StatisticType statisticType;
    private final IntervalDef.Size size;
    private final String dataPointOriginalName;

    protected DataPointTarget(Database database, DataPointProcessor.StatisticType statisticType, IntervalDef.Size size, String dataPointOriginalName) {
        super(database);

        this.statisticType = statisticType;
        this.size = size;
        this.dataPointOriginalName = dataPointOriginalName;

        // Define target data point name
        name = "D_STATISTIC_" + dataPointOriginalName + "_" + statisticType + "_" + size;

        MyLogger.Log(1, new MyLogger.Item("Data point target", -20, name, 0));
    }

    // Create target data point and entry in DATA_POINTS if not existing
    public void prepare(String unit) throws SQLException {
        // Create data point if not existing
        database.execute("create table if not exists " + name + " (TS datetime, \"VALUE\" double, STATE int)");

        // Add data point entry to DATA_POINTS if not contained
        RowSet rowSet = database.executeQuery("select * from DATA_POINTS where TABLE_NAME = '" + name + "'");
        if (!rowSet.next()) {
            String columns = "TABLE_NAME, ";
            String values = "'" + name + "', ";

            rowSet = database.executeQuery("select INTERFACE, ADDRESS, IDENTIFIER, DISPLAY_NAME, ROOM, MAXIMUM, UNIT, MINIMUM, TYPE, DEFAULT_VALUE from DATA_POINTS where TABLE_NAME = '" + dataPointOriginalName + "'");
            if (!rowSet.last() || rowSet.getRow() != 1) {
                throw new IllegalArgumentException("Entry for source data point " + dataPointOriginalName + " not found in DATA_POINTS!");
            }

            columns += "INTERFACE, ";
            values += "'Statistic_" + rowSet.getString(1) + "', ";

            columns += "ADDRESS, ";
            values += "'" + rowSet.getString(2) + "', ";

            columns += "IDENTIFIER, ";
            values += "'" + rowSet.getString(3) + "_" + statisticType + "_" + size + "', ";

            columns += "DISPLAY_NAME, ";
            values += "'" + rowSet.getString(4) + "', ";

            columns += "ROOM, ";
            values += "'" + rowSet.getString(5) + "', ";

            columns += "MAXIMUM, ";
            values += rowSet.getString(6) + ", ";

            columns += "UNIT, ";
            if (unit == null || unit.isEmpty()) {
                values += "'" + rowSet.getString(7) + "', ";
            } else {
                values += "'" + unit + "', ";
            }

            columns += "MINIMUM, ";
            values += rowSet.getString(8) + ", ";

            columns += "TYPE, ";
            values += "'" + rowSet.getString(9) + "', ";

            columns += "DEFAULT_VALUE";
            values += rowSet.getString(10);

            database.execute("insert into DATA_POINTS (" + columns + ") values (" + values + ")");
        }
    }

    public void insertRow(Event event) throws SQLException {
        database.execute("insert into " + name + " (TS, \"VALUE\", STATE) VALUES ('" + MyLogger.GetTimeString(event.timestamp) + "', " + event.value + ", 2)");
    }

    // Delete data point
    public void delete() throws SQLException {
        MyLogger.Log(0, "Deleting data point " + name);

        // Delete data point
        database.execute("drop table if exists " + name);

        // Delete data point entry in DATA_POINTS
        database.execute("delete from DATA_POINTS where TABLE_NAME = '" + name + "'");
    }
}
