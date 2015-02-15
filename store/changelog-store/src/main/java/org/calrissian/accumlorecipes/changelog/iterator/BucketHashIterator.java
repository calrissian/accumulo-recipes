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
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.accumlorecipes.changelog.support.Utils.hashEntry;
import static org.calrissian.accumulorecipes.commons.util.WritableUtils2.asWritable;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class BucketHashIterator extends WrappingIterator {

    protected String currentBucket;
    protected List<String> hashes;
    protected Key retKey;
    protected Value val;
    private TypeRegistry<String> typeRegistry;

    public static void setBucketSize(IteratorSetting is, BucketSize bucketSize) {
        is.addOption("bucketSize", bucketSize.name());
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {

        super.init(source, options, env);
        typeRegistry = LEXI_TYPES;   //TODO make types configurable.
        hashes = new ArrayList<String>();
    }

    @Override
    public boolean hasTop() {
        return val != null || super.hasTop();
    }

    @Override
    public void next() throws IOException {
        primeVal();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        primeVal();
    }

    @Override
    public Key getTopKey() {
        return retKey;
    }

    @Override
    public Value getTopValue() {
        return val;
    }

    public void primeVal() {

        val = null;
        hashes = new ArrayList<String>();

        String nowBucket = currentBucket;
        try {

            while (super.hasTop()) {

                Key topKey = super.getTopKey();
                Value value = super.getTopValue();

                if (currentBucket == null) {
                    currentBucket = topKey.getRow().toString();
                    nowBucket = currentBucket;
                }
                if (!topKey.getRow().toString().equals(currentBucket)) {
                    currentBucket = topKey.getRow().toString();
                    break;
                }

                super.next();

                Event entry = asWritable(value.get(), EventWritable.class).get();
                hashes.add(new String(hashEntry(entry, typeRegistry)));
            }

            if (hashes.size() > 0) {
                val = new Value(md5Hex(join(hashes, ",")).getBytes());
                retKey = new Key(new Text(nowBucket));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
