package de.behms.ccustatistics;

import de.behms.ccustatistics.database.Database;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

public class CcuStatisticsTest {

    // Set CcuHistorian database IP address for tests to be executed!
    final static String HOST = ""; // e.g. 192.168.1.231

    final static String TABLE_NAME = "D_TEST_RF_NEQ1111111_1_DATA";
    static Database database;

    @BeforeAll
    public static void setUpClass() throws Exception {
        Assumptions.assumeFalse(HOST.isEmpty());

        String[] args = {"-host", HOST};
        Arguments arguments = new Arguments(args, true);
        database = new Database(arguments);

        // Delete source data point
        database.execute("drop table if exists " + TABLE_NAME);
        database.execute("delete from DATA_POINTS where TABLE_NAME = '" + TABLE_NAME + "'");

        // Create source data point
        database.execute("create table if not exists " + TABLE_NAME + " (TS datetime, VALUE double, STATE int)");
        database.execute("insert into DATA_POINTS (TABLE_NAME,           INTERFACE, ADDRESS,        IDENTIFIER,       DISPLAY_NAME,         ROOM,                MAXIMUM,         UNIT, MINIMUM, TYPE,    DEFAULT_VALUE) "
                + "                        values ('" + TABLE_NAME + "', 'Test',    'NEQ1111111:1', 'DATA',           'Test Data',          '${roomLivingRoom}', 838860.69921875, 'Wh', 0.0,     'FLOAT', 0.0)");

        // Add test data
        String statement = "insert into " + TABLE_NAME + " VALUES ";
        database.execute(statement + "('2019-12-31 00:00:00.0', 0000.0, 2)");

        database.execute(statement + "('2020-01-01 00:00:00.0', 0000.0, 2)");
        database.execute(statement + "('2020-01-01 00:00:00.1', 4000.0, 2)");

        database.execute(statement + "('2020-01-01 01:30:00.1', 5000.0, 2)");
        database.execute(statement + "('2020-01-01 01:30:00.1', 1000.0, 2)");
        database.execute(statement + "('2020-01-01 01:30:00.1', 2000.0, 2)");

        database.execute(statement + "('2020-01-01 02:00:00.0', 3000.0, 2)");

        database.execute(statement + "('2020-01-01 03:00:00.0', 3000.0, 2)");

        database.execute(statement + "('2020-01-01 04:00:00.0', 1000.0, 2)");

        database.execute(statement + "('2020-01-01 08:00:00.0', 5000.0, 2)");

        database.execute(statement + "('2020-01-01 09:00:00.0', 10000.0, 2)");
        database.execute(statement + "('2020-01-01 09:10:00.0', 20000.0, 2)");
        database.execute(statement + "('2020-01-01 09:20:00.0', 30001.0, 2)");
        database.execute(statement + "('2020-01-01 09:30:00.0', 31000.0, 2)");

        database.execute(statement + "('2020-01-01 10:30:00.0', 32000.0, 2)");

        database.execute(statement + "('2020-01-03 00:00:00.0', 33000.0, 2)");

        database.execute(statement + "('2020-01-03 01:00:00.0', 34000.0, 2)");

        database.commit();
    }

    @AfterAll
    public static void tearDownClass() throws Exception {
        Assumptions.assumeFalse(HOST.isEmpty());

        // Delete source data point
        database.execute("drop table if exists " + TABLE_NAME);
        database.execute("delete from DATA_POINTS where TABLE_NAME = '" + TABLE_NAME + "'");

        database.commit();
    }

    @Test
    public void testRisingOnly() throws SQLException, ClassNotFoundException {
        // Create argument list
        List<String> args = List.of("-host", HOST, "-factor", "0.001", "-unit", "kWh", "-filter", "10000.0", "-interface", "Test", "-address", "NEQ1111111:1", "-identifier", "DATA", "-type", "RISE");

        // Run test
        runTest(args);
    }

    @Test
    public void testOscillating() throws SQLException, ClassNotFoundException {
        // Create argument list
        List<String> args = List.of("-host", HOST, "-interface", "Test", "-address", "NEQ1111111:1", "-identifier", "DATA", "-type", "OSCILL");

        // Run test
        runTest(args);
    }

    private void runTest(List<String> args) {
        // Create arguments to create statistic data
        String[] argsArrayCreate = args.toArray(new String[args.size()]);

        // Create arguments to delete statistic data
        List<String> argsListDelete = new ArrayList<>(args);
        argsListDelete.add("-delete");
        String[] argsArrayDelete = argsListDelete.toArray(new String[argsListDelete.size()]);

        // Delete statistic data
        CcuStatistics.main(argsArrayDelete);

        // Create statistic data
        CcuStatistics.main(argsArrayCreate);

        // Delete statistic data
        CcuStatistics.main(argsArrayDelete);
    }
}
