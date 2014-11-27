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
package org.calrissian.accumulorecipes.eventstore.support.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;

import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.parseTimestampFromKey;

/**
 * A small modification of the age off filter that ships with Accumulo which ages off key/value pairs based on the
 * Key's timestamp. It removes an entry if its timestamp is less than currentTime - threshold.
 * <p/>
 * The modification will now allow rows with timestamp > currentTime to pass through.
 * <p/>
 * This filter requires a "ttl" option, in milliseconds, to determine the age off threshold.
 */
public class EventTimeLimitingFilter extends TimeLimitingFilter {
    @Override
    protected long parseTimestamp(Key k, Value v) {
        return parseTimestampFromKey(k);
    }
}
