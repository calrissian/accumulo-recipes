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
package org.calrissian.accumulorecipes.lastn.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import static org.calrissian.accumulorecipes.lastn.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.lastn.support.Constants.DELIM_END;

/**
 * A cleanup filtering iterator to get rid of tuples that should not exist after the versioning iterator evicts an
 * index. NOTE: This iterator needs to run after the versioning iterator and should run on all scopes (majc,minc,scan).
 */
public class IndexEntryFilteringIterator extends Filter {
    protected HashSet<String> uuidSet = null;
    protected String currentIndex = null;
    protected String previousEvent = null;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        IndexEntryFilteringIterator copy = (IndexEntryFilteringIterator) super.deepCopy(env);
        copy.currentIndex = currentIndex;
        copy.previousEvent = previousEvent;
        copy.uuidSet = new HashSet<String>(uuidSet);

        return copy;
    }


    /**
     * Makes sure only those rows with an index row are kept around. The versioning iterator should have run already
     * and evicted older index rows.
     * @param key
     * @param value
     * @return
     */
    @Override
    public boolean accept(Key key, Value value) {

        try {
            // first find out if we are inside of an index row
            if(key.getColumnFamily().toString().equals(DELIM + "INDEX")) {

                if(!key.getRow().toString().equals(currentIndex)) {
                    currentIndex = key.getRow().toString();
                    uuidSet = new HashSet<String>();
                }

                uuidSet.add(new String(value.get()));

                return true;
            }

            // otherwise, assume we are in an event row
            else {

                String uuid = key.getColumnFamily().toString().replace(DELIM_END, "");
                String hash = new String(value.get());

                if(!uuidSet.contains(uuid + DELIM + hash)) {
                    return false;
                }

                String[] keyValue = key.getColumnQualifier().toString().split(DELIM);

                // here we want to make sure that any duplicate events added are filtered out (this is possible simply
                // because the maxVersions > 1)

                if(previousEvent != null && previousEvent.equals(key.getRow() + DELIM + uuid + DELIM + hash
                        + DELIM + keyValue[0] + DELIM + keyValue[1])) {
                    return false;
                }

                previousEvent = key.getRow() + DELIM + uuid + DELIM + hash + DELIM
                        + keyValue[0] + DELIM + keyValue[1];

            }
        } catch(Exception e) {

            return true;
        }

        return true;
    }
}
