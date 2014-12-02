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
package org.calrissian.accumulorecipes.commons.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A small modification of the age off filter that ships with Accumulo which ages off key/value pairs based on the
 * Key's timestamp. It removes an entry if its timestamp is less than currentTime - threshold.
 * <p/>
 * The modification will now allow rows with timestamp > currentTime to pass through.
 * <p/>
 * This filter requires a "ttl" option, in milliseconds, to determine the age off threshold.
 */
public class TimeLimitingFilter extends Filter {

    protected static final String TTL = "ttl";
    protected static final String CURRENT_TIME = "currentTime";

    protected long threshold;

    /**
     * The use of private for this member in the original AgeOffFilter wouldn't allow me to extend it. Setting to protected.
     */
    protected long currentTime;

    /**
     * A convenience method for setting the age off threshold.
     *
     * @param is  IteratorSetting object to configure.
     * @param ttl age off threshold in milliseconds.
     */
    public static void setTTL(IteratorSetting is, Long ttl) {
        is.addOption(TTL, Long.toString(ttl));
    }

    /**
     * A convenience method for setting the current time (from which to measure the age off threshold).
     *
     * @param is          IteratorSetting object to configure.
     * @param currentTime time in milliseconds.
     */
    public static void setCurrentTime(IteratorSetting is, Long currentTime) {
        is.addOption(CURRENT_TIME, Long.toString(currentTime));
    }

    /**
     * Accepts entries whose timestamps are less than currentTime - threshold.
     *
     * @see org.apache.accumulo.core.iterators.Filter#accept(org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value)
     */
    @Override
    public boolean accept(Key k, Value v) {
        long timestamp = parseTimestamp(k, v);
        if (timestamp > currentTime || currentTime - timestamp > threshold)
            return false;
        return true;
    }

    protected long parseTimestamp(Key k, Value v) {
        return k.getTimestamp();
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        threshold = -1;
        if (options == null)
            throw new IllegalArgumentException(TTL + " must be set for TimeLimitingFilter");

        String ttl = options.get(TTL);
        if (ttl == null)
            throw new IllegalArgumentException(TTL + " must be set for TimelimitingFilter");

        threshold = Long.parseLong(ttl);

        String time = options.get(CURRENT_TIME);
        if (time != null)
            currentTime = Long.parseLong(time);
        else
            currentTime = System.currentTimeMillis();

        // add sanity checks for threshold and currentTime?
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        TimeLimitingFilter copy = (TimeLimitingFilter) super.deepCopy(env);
        copy.currentTime = currentTime;
        copy.threshold = threshold;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(TTL, "time to live (milliseconds)");
        io.addNamedOption(CURRENT_TIME, "if set, use the given value as the absolute time in milliseconds as the current time of day");
        io.setName("ageoff");
        io.setDescription("LimitingAgeOffFilter removes entries with timestamps more than <ttl> milliseconds old & timestamps newer than currentTime");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        super.validateOptions(options);
        try {
            Long.parseLong(options.get(TTL));
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
