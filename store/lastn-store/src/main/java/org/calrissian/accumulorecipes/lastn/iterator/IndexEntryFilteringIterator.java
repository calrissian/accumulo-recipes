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

import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A cleanup filtering iterator to get rid of getAttributes that should not exist after the versioning iterator evicts an
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
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        IndexEntryFilteringIterator copy = (IndexEntryFilteringIterator) super.deepCopy(env);
        copy.currentIndex = currentIndex;
        copy.previousEvent = previousEvent;
        copy.uuidSet = new HashSet<String>(uuidSet);

        return copy;
    }


    /**
     * Makes sure only those rows with an index row are kept around. The versioning iterator should have run already
     * and evicted older index rows.
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public boolean accept(Key key, Value value) {

        try {
            // first find out if we are inside of an index row
            if (key.getColumnFamily().toString().equals(NULL_BYTE + "INDEX")) {

                if (!key.getRow().toString().equals(currentIndex)) {
                    currentIndex = key.getRow().toString();
                    uuidSet = new HashSet<String>();
                }

                uuidSet.add(new String(value.get()));

                return true;
            }

            // otherwise, assume we are in an event row
            else {

                String uuid = key.getColumnFamily().toString().replace(END_BYTE, "");
                String hash = new String(value.get());

                if (!uuidSet.contains(uuid + NULL_BYTE + hash)) {
                    return false;
                }

                String[] keyValue = key.getColumnQualifier().toString().split(NULL_BYTE);

                // here we want to make sure that any duplicate events added are filtered out (this is possible simply
                // because the maxVersions > 1)

                if (previousEvent != null && previousEvent.equals(key.getRow() + NULL_BYTE + uuid + NULL_BYTE + hash
                        + NULL_BYTE + keyValue[0] + NULL_BYTE + keyValue[1])) {
                    return false;
                }

                previousEvent = key.getRow() + NULL_BYTE + uuid + NULL_BYTE + hash + NULL_BYTE
                        + keyValue[0] + NULL_BYTE + keyValue[1];

            }
        } catch (Exception e) {

            return true;
        }

        return true;
    }
}
