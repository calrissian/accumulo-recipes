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
package org.calrissian.accumulorecipes.eventstore.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;

import java.io.IOException;

import static org.calrissian.accumulorecipes.eventstore.iterator.IteratorUtils.retrieveFullEvent;

public class EventIntersectingIterator extends IntersectingIterator {

    protected SortedKeyValueIterator<Key,Value> sourceItr;

    protected Key topKey;

    public void init(SortedKeyValueIterator<Key,Value> source, java.util.Map<String,String> options, IteratorEnvironment env) throws IOException {

        super.init(source, options, env);
        sourceItr = source.deepCopy(env);
    }

    @Override
    public Value getTopValue() {

        if(hasTop()) {

            Key topKey = getTopKey();
            String eventUUID = topKey.getColumnQualifier().toString();

            Value event = retrieveFullEvent(eventUUID, topKey, sourceItr);
            return event;
        }

        return new Value("".getBytes());
    }
}
