package de.behms.ccustatistics;

import de.behms.ccustatistics.DataPointProcessor.StatisticType;
import de.behms.ccustatistics.interval.IntervalDataSum;
import de.behms.ccustatistics.interval.IntervalDataAvg;
import de.behms.ccustatistics.interval.IntervalData;
import de.behms.ccustatistics.interval.IntervalDataMax;
import de.behms.ccustatistics.interval.IntervalDef;
import de.behms.ccustatistics.interval.IntervalDataMin;
import de.behms.ccustatistics.database.DataPointTarget;
import de.behms.ccustatistics.database.Event;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.LinkedList;

public class EventProcessor {

    private final DataPointTarget dataPointTarget;
    private final DataPointProcessor.StatisticType statisticType;
    private final IntervalDef intervalDef;
    private final TimeDef timeDef;
    private final double factor;
    private final Double maxJump;

    private LocalDateTime lastTimestamp = null;
    private double lastValue = 0.0;
    private LocalDateTime lastInterval = null;
    private IntervalData intervalData = null;

    EventProcessor(DataPointTarget dataPointTarget, DataPointProcessor.StatisticType statisticType, IntervalDef intervalDef, TimeDef timeDef, double factor, Double maxJump) {
        this.dataPointTarget = dataPointTarget;
        this.statisticType = statisticType;
        this.intervalDef = intervalDef;
        this.timeDef = timeDef;
        this.factor = factor;
        this.maxJump = maxJump;
    }

    public void processEvents(LinkedList<Event> events) throws SQLException {
        if (statisticType == StatisticType.SUM && intervalDef.getSize() == IntervalDef.Size.HOUR) {
            events = IntervalDataSum.preProcessEvents(events, maxJump);
        }

        if (!events.isEmpty()) {
            MyLogger.Log(0, "Processing events");
            for (Event event : events) {
                if (!processEvent(event)) {
                    break;
                }
            }
        }
    }

    private boolean processEvent(Event event) throws SQLException {
        LocalDateTime currentTimestamp = event.timestamp;
        double currentValue = event.value;

        if (intervalDef.getSize() == IntervalDef.Size.HOUR) {
            // Apply factor only to hour interval
            currentValue *= factor;
        }

        LocalDateTime currentInterval = intervalDef.getIntervalStart(currentTimestamp);

        // Check if first relevant interval is reached (first fetched row(s) must always be located before targetStartTime so last* values will be initialized at first access)
        if (currentTimestamp.isAfter(timeDef.targetStartTime) || currentTimestamp.isEqual(timeDef.targetStartTime)) {
            // Check if a following interval is reached (may have skipped some intervals after last interval)
            if (!currentInterval.isEqual(lastInterval)) {
                // For hour interval some special cases have to be handled
                // - Also create statistic entries for intervals without data if any
                // - For rising only values handle reset or overflow and create sum of differences
                if (intervalDef.getSize() == IntervalDef.Size.HOUR) {
                    // Also create statistic entries for intervals without data if any
                    LocalDateTime statisticInterval = lastInterval;
                    while (!statisticInterval.equals(currentInterval)) {
                        // Calculate this statistic interval end / next statistic interval start
                        LocalDateTime nextStatisticInterval = intervalDef.getNextIntervalStart(statisticInterval);

                        // If there is an interval data object, process it
                        if (intervalData != null) {
                            // Generate and commit statistic data to database
                            intervalData.generateHourStatisticData(dataPointTarget, new Event(currentTimestamp, currentValue));
                        }

                        // Check if all available data was processed
                        if (nextStatisticInterval.isAfter(timeDef.sourceMaxTime)) {
                            return false;
                        }

                        // Create new interval data object
                        switch (statisticType) {
                            case AVG:
                                intervalData = new IntervalDataAvg(nextStatisticInterval, new Event(lastTimestamp, lastValue));
                                break;
                            case MIN:
                                intervalData = new IntervalDataMin(nextStatisticInterval, new Event(lastTimestamp, lastValue));
                                break;
                            case MAX:
                                intervalData = new IntervalDataMax(nextStatisticInterval, new Event(lastTimestamp, lastValue));
                                break;
                            case SUM:
                                intervalData = new IntervalDataSum(nextStatisticInterval, new Event(lastTimestamp, lastValue));
                                break;
                        }

                        // Proceed to next interval
                        statisticInterval = nextStatisticInterval;
                    }
                } else {
                    // If there is an interval data object, process it
                    if (intervalData != null) {
                        // Generate and commit statistic data to database
                        intervalData.generateOtherStatisticData(dataPointTarget);
                    }

                    // Check if all available data was processed
                    if (currentInterval.isAfter(timeDef.sourceMaxTime)) {
                        return false;
                    }

                    // Create new interval data object
                    switch (statisticType) {
                        case AVG:
                            intervalData = new IntervalDataAvg(intervalDef.getSize(), currentInterval);
                            break;
                        case MIN:
                            intervalData = new IntervalDataMin(intervalDef.getSize(), currentInterval);
                            break;
                        case MAX:
                            intervalData = new IntervalDataMax(intervalDef.getSize(), currentInterval);
                            break;
                        case SUM:
                            intervalData = new IntervalDataSum(intervalDef.getSize(), currentInterval);
                            break;
                    }
                }
            }

            // Add event data
            intervalData.addEvent(currentTimestamp, currentValue);
        }

        // Set last to current values
        lastTimestamp = currentTimestamp;
        lastValue = currentValue;
        lastInterval = currentInterval;

        return true;
    }
}
