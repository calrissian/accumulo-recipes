package org.calrissian.accumulorecipes.metricsstore.support;

import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static java.lang.Long.parseLong;

/**
 * Utility class to help with managing timestamps for data being input into accumulo
 */
public class TimestampUtil {

    private TimestampUtil() {}

    private final static DateTimeFormatter MINUTES_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm");
    private final static DateTimeFormatter HOURS_FORMAT = DateTimeFormat.forPattern("yyyyMMddHH");
    private final static DateTimeFormatter DAYS_FORMAT = DateTimeFormat.forPattern("yyyyMMdd");
    private final static DateTimeFormatter MONTHS_FORMAT = DateTimeFormat.forPattern("yyyyMM");

    /**
     * Poor mans reverse function.
     * Simply allows the latest timestamps to appear first in lexigraphical ordering.
     * @param timestamp
     * @return
     */
    private static String reverse(String timestamp){
        return Long.toString(Long.MAX_VALUE - parseLong(timestamp));
    }

    /**
     * Will generate a reverse timestamp with the precision of the provided timeunit.
     * @param timestamp
     * @param timeUnit
     * @return
     */
    public static String generateTimestamp(long timestamp, MetricTimeUnit timeUnit) {
        switch (timeUnit) {
            case MINUTES:
                return reverse(MINUTES_FORMAT.print(timestamp));
            case HOURS:
                return reverse(HOURS_FORMAT.print(timestamp));
            case DAYS:
                return reverse(DAYS_FORMAT.print(timestamp));
            case MONTHS:
                return reverse(MONTHS_FORMAT.print(timestamp));
        }

        // this should really never get thrown
        throw new IllegalArgumentException("Unsupported time unit");
    }

    /**
     * Reverts the string timestamp into an epoch timestamp with the precision of the provided timeunit.
     * @param timestamp
     * @param timeUnit
     * @return
     */
    public static long revertTimestamp(String timestamp, MetricTimeUnit timeUnit) {
        switch (timeUnit) {
            case MINUTES:
                return MINUTES_FORMAT.parseMillis(reverse(timestamp));
            case HOURS:
                return HOURS_FORMAT.parseMillis(reverse(timestamp));
            case DAYS:
                return DAYS_FORMAT.parseMillis(reverse(timestamp));
            case MONTHS:
                return MONTHS_FORMAT.parseMillis(reverse(timestamp));
        }

        // this should really never get thrown
        throw new IllegalArgumentException("Unsupported time unit");
    }

}
