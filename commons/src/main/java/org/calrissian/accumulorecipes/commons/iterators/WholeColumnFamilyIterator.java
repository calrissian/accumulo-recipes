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
package org.calrissian.accumulorecipes.commons.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.util.RowEncoderUtil;

public class WholeColumnFamilyIterator implements SortedKeyValueIterator<Key, Value> {


    List<Map.Entry<Key,Value>> keysValues = Lists.newArrayList();
    private SortedKeyValueIterator<Key, Value> sourceIter;
    private Key topKey = null;
    private Value topValue = null;

    public WholeColumnFamilyIterator() {

    }

    WholeColumnFamilyIterator(SortedKeyValueIterator<Key, Value> source) {
        this.sourceIter = source;
    }

    private void prepKeys() throws IOException {
        if (topKey != null)
            return;
        Text currentRow;
        Text currentCF;
        long curTS;
        do {
            if (sourceIter.hasTop() == false)
                return;
            currentRow = new Text(sourceIter.getTopKey().getRow());
            currentCF = new Text(sourceIter.getTopKey().getColumnFamily());
            curTS = sourceIter.getTopKey().getTimestamp();
            keysValues.clear();
            while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow) &&
                sourceIter.getTopKey().getColumnFamily().equals(currentCF)) {
                keysValues.add(Maps.immutableEntry(new Key(sourceIter.getTopKey()), new Value(sourceIter.getTopValue())));
                sourceIter.next();
            }
        } while (!filter(currentRow, keysValues));

        topKey = new Key(currentRow, currentCF, new Text(""), curTS);
        topValue = RowEncoderUtil.encodeRow(keysValues);

    }

    /**
     * @param currentRow All keys have this in their row portion (do not modify!).
     * @param keys       One key for each key in the row, ordered as they are given by the source iterator (do not modify!).
     * @param values     One value for each key in keys, ordered to correspond to the ordering in keys (do not modify!).
     * @return true if we want to keep the row, false if we want to skip it
     */
    protected boolean filter(Text currentRow, Collection<Map.Entry<Key,Value>> keysValues) {
        return true;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (sourceIter != null)
            return new WholeColumnFamilyIterator(sourceIter.deepCopy(env));
        return new WholeColumnFamilyIterator();
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return topKey != null;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        sourceIter = source;
    }

    @Override
    public void next() throws IOException {
        topKey = null;
        topValue = null;
        prepKeys();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        topKey = null;
        topValue = null;

        Key sk = range.getStartKey();

        if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
            && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
            // assuming that we are seeking using a key previously returned by this iterator
            // therefore go to the next row
            Key followingRowKey = sk.followingKey(PartialKey.ROW);
            if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
                return;

            range = new Range(sk.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
        }

        sourceIter.seek(range, columnFamilies, inclusive);
        prepKeys();
    }

}
