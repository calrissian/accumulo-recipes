/*
 * Copyright (C) 2015 The Calrissian Authors
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

import java.io.IOException;
import java.util.HashMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;
import org.calrissian.accumulorecipes.commons.support.Constants;

public class EventMetadataExpirationFilter extends MetadataExpirationFilter{
    @Override
    protected long parseTimestampFromKey(Key k) {

        String cf = k.getColumnFamily().toString();
        int idx = cf.lastIndexOf(Constants.ONE_BYTE);
        return Long.parseLong(cf.substring(idx+1, cf.length()));
    }


    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EventMetadataExpirationFilter ret = new EventMetadataExpirationFilter();
        try {
            ret.init(getSource(), new HashMap<String,String>(), env);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
