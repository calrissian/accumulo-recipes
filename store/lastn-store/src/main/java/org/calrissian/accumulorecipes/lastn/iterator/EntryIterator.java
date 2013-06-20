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
package org.calrissian.accumulorecipes.lastn.iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.serialization.ObjectMapperContext;
import org.calrissian.mango.types.TypeContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.calrissian.accumulorecipes.lastn.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.lastn.support.Constants.DELIM_END;

/**
 * An iterator to return StoreEntry objects serialized to JSON so that grouping can be done server side instead of
 * client side.
 */
public class EntryIterator extends WrappingIterator {

    protected SortedKeyValueIterator<Key,Value> sourceItr;

    public void init(SortedKeyValueIterator<Key,Value> source, java.util.Map<String,String> options,
                     IteratorEnvironment env) throws IOException {

        super.init(source, options, env);
        sourceItr = source.deepCopy(env);
    }

    /**
     * For each index row in the lastN store, grab the associated tuples (further down in the tablet) and construct
     * the entry to be returned.
     * @return
     */
    @Override
    public Value getTopValue() {

        if(hasTop()) {

            Key topKey = getTopKey();
            Value topVal = super.getTopValue();
            String entryId = new String(topVal.get());

            Key startRangeKey = new Key(topKey.getRow(), new Text(DELIM_END + entryId));
            Key stopRangeKey = new Key(topKey.getRow(), new Text(DELIM_END + entryId + DELIM_END));

            Range range = new Range(startRangeKey, stopRangeKey);

            long timestamp = 0;

            try {
                sourceItr.seek(range, new ArrayList<ByteSequence>(), false);

                Collection<Tuple> tuples = new ArrayList<Tuple>();
                while(sourceItr.hasTop()) {

                    Key nextKey = sourceItr.getTopKey();
                    sourceItr.next();

                    if(!nextKey.getColumnFamily().toString().endsWith(entryId)) {
                        break;
                    }

                    String[] keyValueDatatype = nextKey.getColumnQualifier().toString().split(DELIM);

                    if(keyValueDatatype.length == 3) {

                        String tupleKey = keyValueDatatype[0];
                        String tupleType = keyValueDatatype[2];
                        Object tupleVal = TypeContext.getInstance().denormalize(keyValueDatatype[1], tupleType);

                        Tuple tuple = new Tuple(tupleKey, tupleVal, nextKey.getColumnVisibility().toString());
                        tuples.add(tuple);

                        timestamp = nextKey.getTimestamp();
                    }
                }

                StoreEntry entry = new StoreEntry(entryId, timestamp);

                if(tuples.size() > 0) {
                    entry.putAll(tuples);
                }

                return new Value(ObjectMapperContext.getInstance().getObjectMapper().writeValueAsBytes(entry));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new Value("".getBytes());
    }
}
