package de.behms.ccustatistics.database;

import java.sql.SQLException;
import javax.sql.RowSet;

public class TimeSeries {

    private final RowSet rowSet;

    public Event next() throws SQLException {
        if (rowSet.next()) {
            return new Event(rowSet.getTimestamp(1).toLocalDateTime(), rowSet.getDouble(2));
        } else {
            return null;
        }
    }

    protected TimeSeries(RowSet rowSet) {
        this.rowSet = rowSet;
    }
}
