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

import org.calrissian.mango.types.encoders.lexi.LongReverseEncoder;

/**
 * Utility class to help with managing timestamps for data being input into accumulo
 */
public class TimestampUtil {

    private static LongReverseEncoder encoder = new LongReverseEncoder();

    private TimestampUtil() {
    }

    /**
     * Will generate a reverse timestamp with the precision of the provided timeunit.
     *
     * @param timestamp
     * @param timeUnit
     * @return
     */
    public static String generateTimestamp(long timestamp, TimeUnit timeUnit) {
        return encoder.encode(timeUnit.normalize(timestamp));
    }

    /**
     * Reverts the string timestamp into an epoch timestamp with the precision of the provided timeunit.
     *
     * @param timestamp
     * @return
     */
    public static long revertTimestamp(String timestamp) {
        return encoder.decode(timestamp);
    }

}
