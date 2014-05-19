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
package org.calrissian.accumlorecipes.changelog.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.support.BucketSize;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.calrissian.accumlorecipes.changelog.support.Utils.reverseTimestampToNormalTime;
import static org.calrissian.accumlorecipes.changelog.support.Utils.truncatedReverseTimestamp;

public class BucketRollupIterator extends WrappingIterator {

    protected BucketSize bucketSize = BucketSize.FIVE_MINS;

    public static void setBucketSize(IteratorSetting is, BucketSize bucketSize) {

        is.addOption("bucketSize", bucketSize.name());
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey("bucketSize")) {
            bucketSize = BucketSize.valueOf(options.get("bucketSize"));
        }
    }

    @Override
    public boolean hasTop() {
        return super.hasTop();
    }

    @Override
    public void next() throws IOException {
        super.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        Key topKey = super.getTopKey();

        long timestamp = reverseTimestampToNormalTime(Long.parseLong(topKey.getRow().toString()));

        Key retKey = new Key(new Text(truncatedReverseTimestamp(timestamp, bucketSize).toString()),
                topKey.getColumnFamily(), topKey.getColumnQualifier(),
                new Text(topKey.getColumnVisibility().toString()), topKey.getTimestamp());

        return retKey;
    }

    @Override
    public Value getTopValue() {
        return super.getTopValue();
    }
}
