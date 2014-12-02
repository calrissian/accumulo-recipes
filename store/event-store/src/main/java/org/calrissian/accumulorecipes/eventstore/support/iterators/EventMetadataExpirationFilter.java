/*
 * Copyright (C) 2014 The Calrissian Authors
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

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;

import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.parseTimestampFromKey;

/**
 * Allows Accumulo to expire keys/values based on an expiration threshold encoded in a metadata map in the value.
 * This is an abstract class that allows the actual fetching of the timestamp from the key/value to be supplied
 * by subclasses.
 */
public class EventMetadataExpirationFilter extends MetadataExpirationFilter {

    @Override
    protected long parseTimestamp(Key k, Map<String,Object> meta) {
        return parseTimestampFromKey(k);
    }
}
