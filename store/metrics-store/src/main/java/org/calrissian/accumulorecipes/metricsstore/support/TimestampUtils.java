package org.calrissian.accumulorecipes.metricsstore.support;

import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampUtils {

    public static final Long MAX_MINUTE = 999999999999l;
    public static final String MINUTE_FORMAT = "yyyyMMddHHmm";

    public static Long truncatedReverseTimestamp(long timestamp, MetricTimeUnit timeUnit) {
        String minutes = new SimpleDateFormat(MINUTE_FORMAT).format(new Date(timestamp));
        Long l = Long.parseLong(minutes);
        long revTs = MAX_MINUTE - l;
        switch (timeUnit) {
            case MINUTES:
                return revTs;
            case HOURS:
                return revTs / 100;
            case DAYS:
                return revTs / 10000;
            case MONTHS:
                return revTs / 1000000;
        }

        // this should really never get thrown
        throw new IllegalArgumentException("Unsupported time unit");
    }

}
