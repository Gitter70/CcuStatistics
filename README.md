### CcuStatistics

#### Create and update CCU-Historian Min/Max/Avg/Sum statistic data points for hours/days/weeks/months/years

##### Usage

    CcuStatistics [-host <address>] [-port <number>] [-dir <location>] [-db <name>] [-user <name>] [-pw <password>] [-factor <value>] [-unit <name>] [-filter <max value difference>] [-delete] -interface <name> -address <id> -identifier <id> -type <OSCILL|RISE>

##### Parameters

    -host <address>                  Address where CCU-Historian is running, default: localhost
    -port <number>                   Network port of the database interface, default: 9092
    -dir <location>                  Database directory location, default: ./data
    -db <name>                       Database name, default: history
    -user <name>                     Database user, default: sa
    -pw <password>                   Database password, default: ccu-historian
    -factor <value>                  Target data point values will be multiplied by this factor (e.g. 0.001), default: 1.0
    -unit <name>                     Target data point unit name (e.g. kWh), default: Unit name of the source (e.g. Wh)
    -filter <max value difference>   For data points of type RISE (e.g. RAIN_COUNTER) there may be large value jumps in the database - all jumps larger than this difference are ignored
    -delete                          Delete target data points instead of creating or updating them
    -interface <name>                CCU-Interface of the device (BidCos-RF, BidCos-Wired, System or SysVar)
    -address <id>                    Serial number: Channel number or system variable ID
    -identifier <id>                 Data point identifier
    -type <OSCILL|RISE>              OSCILL : Values of the data source may rise and fall (e.g. temperatures) - data points for min, max and average values will be created
                                     RISE   : Values of the data source may only rise (except for overflows, e.g. energy counter) - data points for summed differential values will be created

##### E.g.

    java -jar CcuStatistics-1.0.jar -host localhost -interface "BidCos-RF" -address "NEQ0862251:1" -identifier "ENERGY_COUNTER" -type "RISE" -factor 0.001 -unit "kWh"
    java -jar CcuStatistics-1.0.jar -host localhost -interface "BidCos-RF" -address "NEQ0294011:1" -identifier "TEMPERATURE"    -type "OSCILL"
    java -jar CcuStatistics-1.0.jar -host localhost -interface "BidCos-RF" -address "NEQ0294011:1" -identifier "RAIN_COUNTER"   -type "RISE" -filter 2000
