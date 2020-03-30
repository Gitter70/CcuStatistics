package de.behms.ccustatistics.database;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import javax.sql.RowSet;

public abstract class DataPoint {

    protected Database database;
    protected String name;

    public DataPoint(Database database) {
        this.database = database;
    }

    public TimeSeries getTimeSeries(LocalDateTime begin, LocalDateTime end) throws SQLException {
        String sql = "SELECT * FROM " + name + createTimeCondition(begin, end) + " order by TS";
        return new TimeSeries(database.executeQuery(sql));
    }

    public LocalDateTime getMinTimestamp(LocalDateTime begin) throws SQLException {
        return getMinMaxTimestamp("min", createTimeCondition(begin, null));
    }

    public LocalDateTime getMaxTimestamp(LocalDateTime end) throws SQLException {
        return getMinMaxTimestamp("max", createTimeCondition(null, end));
    }

    private LocalDateTime getMinMaxTimestamp(String function, String condition) throws SQLException {
        String sql = "select " + function + "(TS) from " + name + condition;
        RowSet rowSet = database.executeQuery(sql);

        rowSet.next();
        Timestamp timestamp = rowSet.getTimestamp(1);
        if (timestamp == null) {
            return null;
        }

        return timestamp.toLocalDateTime();
    }

    private String createTimeCondition(LocalDateTime begin, LocalDateTime end) {
        String condition = "";

        if (begin != null) {
            condition = " WHERE TS >= '" + Timestamp.valueOf(begin) + "'";
        }

        if (end != null) {
            if (condition.isEmpty()) {
                condition += " WHERE";
            } else {
                condition += " AND";
            }

            condition += " TS < '" + Timestamp.valueOf(end) + "'";
        }

        return condition;
    }
}
