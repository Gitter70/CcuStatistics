package de.behms.ccustatistics;

import de.behms.ccustatistics.database.DatabaseDataPoint;
import java.sql.SQLException;

public class CcuStatistics {

    public static void main(String[] args) {
        // Read command line argumens
        Arguments arguments = new Arguments(args, false);

        // Set log level
        MyLogger.SetLogLevel(arguments.getLog());

        // Connect to databse and process
        try (DatabaseDataPoint databaseDataPoint = new DatabaseDataPoint(arguments)) {
            if (arguments.getType() == Arguments.Type.OSCILL) {
                DataPointProcessor.StatisticType statisticTypes[] = {DataPointProcessor.StatisticType.AVG, DataPointProcessor.StatisticType.MIN, DataPointProcessor.StatisticType.MAX};
                for (var statisticType : statisticTypes) {
                    new DataPointProcessor(arguments, databaseDataPoint, statisticType).processDataPoint();
                }
            } else {
                new DataPointProcessor(arguments, databaseDataPoint, DataPointProcessor.StatisticType.SUM).processDataPoint();
            }

            // Commit changes
            databaseDataPoint.commit();
        } catch (ClassNotFoundException | SQLException ex) {
            MyLogger.LogException("Database error", ex);
        } catch (IllegalArgumentException ex) {
            MyLogger.LogException("Processing error", ex);
        } catch (Exception ex) {
            MyLogger.LogException("Program error", ex);
        }
    }
}
