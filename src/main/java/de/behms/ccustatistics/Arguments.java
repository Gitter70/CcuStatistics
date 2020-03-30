package de.behms.ccustatistics;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Arguments {

    public enum Type {
        OSCILL, RISE
    }

    private final String DEFAULT_HOST = "localhost";
    private final String DEFAULT_PORT = "9092";
    private final String DEFAULT_DIR = "./data";
    private final String DEFAULT_DB = "history";
    private final String DEFAULT_PW = "ccu-historian";
    private final String DEFAULT_FACTOR = "1.0";
    private final String DEFAULT_USER = "sa";

    private CommandLine commandLine;

    Type type = null;

    Arguments(String[] args, boolean allOptional) {
        // Create the command line parser
        CommandLineParser parser = new DefaultParser();

        // Create the Options
        Options options = new Options();

        // Add database arguments
        options.addOption(Option.builder("host")
                .hasArg()
                .argName("address")
                .desc("Address where CCU-Historian is running, default: " + DEFAULT_HOST)
                .build());

        options.addOption(Option.builder("port")
                .hasArg()
                .argName("number")
                .desc("Network port of the database interface, default: " + DEFAULT_PORT)
                .type(Number.class)
                .build());

        options.addOption(Option.builder("dir")
                .hasArg()
                .argName("location")
                .desc("Database directory location, default: " + DEFAULT_DIR)
                .build());

        options.addOption(Option.builder("db")
                .hasArg()
                .argName("name")
                .desc("Database name, default: " + DEFAULT_DB)
                .build());

        options.addOption(Option.builder("user")
                .hasArg()
                .argName("name")
                .desc("Database user, default: " + DEFAULT_USER)
                .build());

        options.addOption(Option.builder("pw")
                .hasArg()
                .argName("password")
                .desc("Database password, default: " + DEFAULT_PW)
                .build());

        // Add target arguments
        options.addOption(Option.builder("factor")
                .hasArg()
                .argName("value")
                .desc("Target data point values will be multiplied by this factor (e.g. 0.001), default: " + DEFAULT_FACTOR)
                .type(Number.class)
                .build());

        options.addOption(Option.builder("unit")
                .hasArg()
                .argName("name")
                .desc("Target data point unit name (e.g. kWh), default: Unit name of the source (e.g. Wh)")
                .build());

        options.addOption(Option.builder("filter")
                .hasArg()
                .argName("max value difference")
                .desc("For data points of type RISE (e.g. RAIN_COUNTER) there may be large value jumps in the database - all jumps larger than this difference are ignored")
                .type(Number.class)
                .build());

        options.addOption(Option.builder("delete")
                .desc("Delete target data points instead of creating or updating them")
                .build());

        // Add source arguments
        options.addOption(Option.builder("interface")
                .hasArg()
                .argName("name")
                .desc("CCU-Interface of the device (BidCos-RF, BidCos-Wired, System or SysVar)")
                .required(!allOptional)
                .build());

        options.addOption(Option.builder("address")
                .hasArg()
                .argName("id")
                .desc("Serial number: Channel number or system variable ID")
                .required(!allOptional)
                .build());

        options.addOption(Option.builder("identifier")
                .hasArg()
                .argName("id")
                .desc("Data point identifier")
                .required(!allOptional)
                .build());

        // Add content type
        options.addOption(Option.builder("type")
                .hasArg()
                .argName("OSCILL|RISE")
                .desc("OSCILL : Values of the data source may rise and fall (e.g. temperatures) - data points for min, max and average values will be created\n"
                        + "RISE   : Values of the data source may only rise (except for overflows, e.g. energy counter) - data points for summed differential values will be created")
                .required(!allOptional)
                .build());

        // Parse the command line arguments
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("type")) {
                switch (commandLine.getOptionValue("type")) {
                    case "OSCILL":
                        type = Type.OSCILL;
                        break;
                    case "RISE":
                        type = Type.RISE;
                        break;
                    default:
                        throw new ParseException("Invalid type provided");
                }
            }
        } catch (ParseException ex) {
            // Show help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(null);
            formatter.printHelp(300, "CcuStatistics", "Create duration specific data points from raw data point", options, "", true);

            System.exit(0);
        }
    }

    public String getHost() {
        return commandLine.getOptionValue("host", DEFAULT_HOST);
    }

    public String getPort() {
        return commandLine.getOptionValue("port", DEFAULT_PORT);
    }

    public String getDir() {
        return commandLine.getOptionValue("dir", DEFAULT_DIR);
    }

    public String getDb() {
        return commandLine.getOptionValue("db", DEFAULT_DB);
    }

    public String getUser() {
        return commandLine.getOptionValue("user", DEFAULT_USER);
    }

    public String getPw() {
        return commandLine.getOptionValue("pw", DEFAULT_PW);
    }

    public double getFactor() {
        return Double.parseDouble(commandLine.getOptionValue("factor", DEFAULT_FACTOR));
    }

    public String getUnit() {
        return commandLine.getOptionValue("unit");
    }

    public Double getFilter() {
        if (commandLine.hasOption("filter")) {
            return Double.parseDouble(commandLine.getOptionValue("filter"));
        } else {
            return null;
        }
    }

    public boolean getDelete() {
        return commandLine.hasOption("delete");
    }

    public String getInterface() {
        return commandLine.getOptionValue("interface");
    }

    public String getAddress() {
        return commandLine.getOptionValue("address");
    }

    public String getIdentifier() {
        return commandLine.getOptionValue("identifier");
    }

    public Type getType() {
        return type;
    }
}
