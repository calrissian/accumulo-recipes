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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.serialization.TupleModule;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;

public class IteratorUtils {

    public static Value retrieveFullEvent(String eventUUID, Key topKey, SortedKeyValueIterator<Key,Value> sourceItr,
                                          TypeRegistry<String> typeRegistry) {

        Key key = topKey;

        Key startRangeKey = new Key(key.getRow(), new Text(SHARD_PREFIX_F +
                DELIM +
                eventUUID));
        Key stopRangeKey = new Key(key.getRow(), new Text(SHARD_PREFIX_F +
                DELIM +
                eventUUID + DELIM_END));

        Range eventRange = new Range(startRangeKey, stopRangeKey);

        long timestamp = 0;

        try {
            sourceItr.seek(eventRange, Collections.<ByteSequence>emptyList(), false);

            Collection<Tuple> tuples = new ArrayList<Tuple>();
            while(sourceItr.hasTop()) {

                Key nextKey = sourceItr.getTopKey();
                sourceItr.next();

                if(!nextKey.getColumnFamily().toString().endsWith(eventUUID)) {
                    break;
                }

                String[] keyValueDatatype = nextKey.getColumnQualifier().toString().split(DELIM);

                if(keyValueDatatype.length == 3) {

                    tuples.add(new Tuple(
                            keyValueDatatype[0],
                            typeRegistry.decode(keyValueDatatype[1], keyValueDatatype[2]),
                            nextKey.getColumnVisibility().toString()));

                    timestamp = nextKey.getTimestamp();
                }
            }

            StoreEntry event = new StoreEntry(eventUUID, timestamp);

            if(tuples.size() > 0)
                event.putAll(tuples);

            return new Value(new ObjectMapper().withModule(new TupleModule(typeRegistry)).writeValueAsBytes(event));

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
