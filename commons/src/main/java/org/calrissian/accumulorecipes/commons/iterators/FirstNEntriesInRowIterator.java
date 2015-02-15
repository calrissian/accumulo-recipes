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

import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.encodeRow;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * This {@link org.apache.accumulo.core.iterators.SortedKeyValueIterator} will return only the first n
 * keys/values for each row that it iterates through.
 */
public class FirstNEntriesInRowIterator implements OptionDescriber, SortedKeyValueIterator<Key, Value> {

    private SortedKeyValueIterator<Key,Value> source = null;

    // options
    static final String NUM_SCANS_STRING_NAME = "scansBeforeSeek";
    static final String NUM_KEYS_STRING_NAME = "n";

    // iterator predecessor seek options to pass through
    private Range latestRange;
    private Collection<ByteSequence> latestColumnFamilies;
    private boolean latestInclusive;

    // private fields
    private Text lastRowFound;
    private int numscans;

    private Key topKey;
    private Value topValue;

    private Collection<Map.Entry<Key,Value>> keysValues = Lists.newArrayList();

    private boolean hasSeeked = false;

    private int n;

    public static void setNumScansBeforeSeek(IteratorSetting cfg, int num) {
        cfg.addOption(NUM_SCANS_STRING_NAME, Integer.toString(num));
    }

    public static void setNumKeysToReturn(IteratorSetting cfg, int n) {
        cfg.addOption(NUM_KEYS_STRING_NAME, Integer.toString(n));
    }

    // this must be public for OptionsDescriber
    public FirstNEntriesInRowIterator() {
    }

    public FirstNEntriesInRowIterator(FirstNEntriesInRowIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
    }

    protected void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new FirstNEntriesInRowIterator(this, env);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        String o = options.get(NUM_SCANS_STRING_NAME);
        String nStr = options.get(NUM_KEYS_STRING_NAME);
        n = nStr == null ? 25 : Integer.parseInt(nStr);
        numscans = o == null ? 10 : Integer.parseInt(o);
        setSource(source.deepCopy(env));
    }


    private void prepKeys() throws IOException {
        if(lastRowFound == null) {
            topKey = null;
            topValue = null;
            return;
        }

        keysValues.clear();

        while(getSource().hasTop() && keysValues.size() < n && getSource().getTopKey().getRow().equals(lastRowFound)) {
            keysValues.add(Maps.immutableEntry(new Key(getSource().getTopKey()), new Value(getSource().getTopValue())));
            getSource().next();
        }

        topKey = new Key(lastRowFound, keysValues.iterator().next().getKey().getColumnFamily());
        topValue = encodeRow(keysValues);
    }

    private boolean finished = true;

    @Override
    public boolean hasTop() {
        return !finished && (topKey != null || getSource().hasTop());
    }

    @Override
    public void next() throws IOException {
        skipRow();
        prepKeys();
    }

    protected SortedKeyValueIterator<Key,Value> getSource() {
        if (source == null)
            throw new IllegalStateException("getting null source");
        return source;
    }

    // this is only ever called immediately after getting "next" entry
    protected void skipRow() throws IOException {
        if (finished == true || lastRowFound == null)
            return;
        int count = 0;
        while (getSource().hasTop() && lastRowFound.equals(getSource().getTopKey().getRow())) {

            // try to efficiently jump to the next matching key
            if (count < numscans) {
                ++count;
                getSource().next(); // scan
            } else {
                // too many scans, just seek
                count = 0;

                // determine where to seek to, but don't go beyond the user-specified range
                Key nextKey = getSource().getTopKey().followingKey(PartialKey.ROW);
                if (!latestRange.afterEndKey(nextKey))
                    getSource().seek(new Range(nextKey, true, latestRange.getEndKey(), latestRange.isEndKeyInclusive()), latestColumnFamilies, latestInclusive);
                else
                {
                    finished = true;
                    break;
                }
            }
        }
        lastRowFound = getSource().hasTop() ? getSource().getTopKey().getRow(lastRowFound) : null;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // save parameters for future internal seeks
        latestRange = range;
        latestColumnFamilies = columnFamilies;
        latestInclusive = inclusive;
        lastRowFound = null;

        Key startKey = range.getStartKey();
        Range seekRange = new Range(startKey == null ? null : new Key(startKey.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());
        getSource().seek(seekRange, columnFamilies, inclusive);
        finished = false;

        if (getSource().hasTop()) {
            lastRowFound = getSource().getTopKey().getRow();
            if (hasSeeked && range.beforeStartKey(getSource().getTopKey()))
                skipRow();
        }

        hasSeeked = true;

        prepKeys();
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
    public IteratorOptions describeOptions() {
        String name = "firstNEntriesInRowIterator";
        String desc = "Only allows iteration over the first n entries per row";
        HashMap<String,String> namedOptions = new HashMap<String,String>();
        namedOptions.put(NUM_SCANS_STRING_NAME, "Number of scans to try before seeking [10]");
        namedOptions.put(NUM_KEYS_STRING_NAME, "Number of entries to keep per row [25]");
        return new IteratorOptions(name, desc, namedOptions, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        try {
            String o = options.get(NUM_SCANS_STRING_NAME);
            if (o != null)
                Integer.parseInt(o);

            String nStr = options.get(NUM_KEYS_STRING_NAME);
            if(nStr != null)
                Integer.parseInt(nStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("bad integer " + NUM_SCANS_STRING_NAME + ":" + options.get(NUM_SCANS_STRING_NAME), e);
        }
        return true;
    }



}
