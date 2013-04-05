/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
