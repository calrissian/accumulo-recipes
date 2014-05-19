/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.support;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static java.lang.Long.parseLong;

/**
 * Utility class to help with managing timestamps for data being input into accumulo
 */
public class TimestampUtil {

    private final static DateTimeFormatter MINUTES_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm");
    private final static DateTimeFormatter HOURS_FORMAT = DateTimeFormat.forPattern("yyyyMMddHH");
    private final static DateTimeFormatter DAYS_FORMAT = DateTimeFormat.forPattern("yyyyMMdd");
    private final static DateTimeFormatter MONTHS_FORMAT = DateTimeFormat.forPattern("yyyyMM");
    private TimestampUtil() {
    }

    /**
     * Poor mans reverse function.
     * Simply allows the latest timestamps to appear first in lexigraphical ordering.
     *
     * @param timestamp
     * @return
     */
    private static String reverse(String timestamp) {
        return Long.toString(Long.MAX_VALUE - parseLong(timestamp));
    }

    /**
     * Will generate a reverse timestamp with the precision of the provided timeunit.
     *
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
     *
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
