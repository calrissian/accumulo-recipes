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

import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public enum TimeUnit {
    MINUTES,
    HOURS,
    DAYS,
    MONTHS;

    public long normalize(long timestamp) {
        MutableDateTime ts = new MutableDateTime(timestamp, DateTimeZone.UTC);

        /**
         * NOTE: order of switch matters.
         *
         * This switch is designed to fall through from most to least significant. Zeroes all non significant
         * portions of the time before finally breaking at the end.
         */
        switch (this) {
            case MONTHS:
                ts.setDayOfMonth(1);
            case DAYS:
                ts.setHourOfDay(0);
            case HOURS:
                ts.setMinuteOfHour(0);
            case MINUTES:
                ts.setSecondOfMinute(0);
                ts.setMillisOfSecond(0);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit");
        }

        return ts.getMillis();
    }


}
