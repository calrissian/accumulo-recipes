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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.*;

import static com.google.common.collect.Maps.immutableEntry;

/**
 * This {@link org.apache.accumulo.core.iterators.SortedKeyValueIterator} will return only the first n
 * keys/values for each row that it iterates through.
 */
public class FirstNEntriesInRowIterator implements OptionDescriber, SortedKeyValueIterator<Key, Value> {

    // options
    static final String NUM_SCANS_STRING_NAME = "scansBeforeSeek";
    static final String NUM_KEYS_STRING_NAME = "n";
    private SortedKeyValueIterator<Key, Value> source = null;
    // iterator predecessor seek options to pass through
    private Range latestRange;
    private Collection<ByteSequence> latestColumnFamilies;
    private boolean latestInclusive;

    // private fields
    private Text lastRowFound;
    private int numscans;

    private Key topKey;
    private Value topValue;

    private List<Key> keys = new ArrayList<Key>();
    private List<Value> values = new ArrayList<Value>();

    private int n;
    private boolean finished = true;

    // this must be public for OptionsDescriber
    public FirstNEntriesInRowIterator() {
    }

    public FirstNEntriesInRowIterator(FirstNEntriesInRowIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
    }

    public static void setNumScansBeforeSeek(IteratorSetting cfg, int num) {
        cfg.addOption(NUM_SCANS_STRING_NAME, Integer.toString(num));
    }

    public static void setNumKeysToReturn(IteratorSetting cfg, int n) {
        cfg.addOption(NUM_KEYS_STRING_NAME, Integer.toString(n));
    }

    // decode a bunch of key value pairs that have been encoded into a single value
    public static final List<Map.Entry<Key, Value>> decodeRow(Key rowKey, Value rowValue) throws IOException {
        List<Map.Entry<Key, Value>> map = new ArrayList<Map.Entry<Key, Value>>();
        ByteArrayInputStream in = new ByteArrayInputStream(rowValue.get());
        DataInputStream din = new DataInputStream(in);
        int numKeys = din.readInt();

        for (int i = 0; i < numKeys; i++) {
            byte[] cf;
            byte[] cq;
            byte[] cv;
            byte[] valBytes;
            // read the col fam
            {
                int len = din.readInt();
                cf = new byte[len];
                din.read(cf);
            }
            // read the col qual
            {
                int len = din.readInt();
                cq = new byte[len];
                din.read(cq);
            }
            // read the col visibility
            {
                int len = din.readInt();
                cv = new byte[len];
                din.read(cv);
            }
            // read the timestamp
            long timestamp = din.readLong();
            // read the value
            {
                int len = din.readInt();
                valBytes = new byte[len];
                din.read(valBytes);
            }
            map.add(immutableEntry(new Key(rowKey.getRowData().toArray(), cf, cq, cv, timestamp, false, false), new Value(valBytes, false)));
        }
        return map;
    }

    // take a stream of keys and values and output a value that encodes everything but their row
    // keys and values must be paired one for one
    public static final Value encodeRow(List<Key> keys, List<Value> values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeInt(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            Key k = keys.get(i);
            Value v = values.get(i);
            // write the colfam
            {
                ByteSequence bs = k.getColumnFamilyData();
                dout.writeInt(bs.length());
                dout.write(bs.getBackingArray(), bs.offset(), bs.length());
            }
            // write the colqual
            {
                ByteSequence bs = k.getColumnQualifierData();
                dout.writeInt(bs.length());
                dout.write(bs.getBackingArray(), bs.offset(), bs.length());
            }
            // write the column visibility
            {
                ByteSequence bs = k.getColumnVisibilityData();
                dout.writeInt(bs.length());
                dout.write(bs.getBackingArray(), bs.offset(), bs.length());
            }
            // write the timestamp
            dout.writeLong(k.getTimestamp());
            // write the value
            byte[] valBytes = v.get();
            dout.writeInt(valBytes.length);
            dout.write(valBytes);
        }

        return new Value(out.toByteArray());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new FirstNEntriesInRowIterator(this, env);
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        String o = options.get(NUM_SCANS_STRING_NAME);
        String nStr = options.get(NUM_KEYS_STRING_NAME);
        n = nStr == null ? 25 : Integer.parseInt(nStr);
        numscans = o == null ? 10 : Integer.parseInt(o);
        setSource(source.deepCopy(env));
    }

    private void prepKeys() throws IOException {
        if (lastRowFound == null) {
            topKey = null;
            topValue = null;
            return;
        }

        keys.clear();
        values.clear();

        while (getSource().hasTop() && keys.size() < n && getSource().getTopKey().getRow().equals(lastRowFound)) {
            keys.add(new Key(getSource().getTopKey()));
            values.add(new Value(getSource().getTopValue()));
            getSource().next();
        }

        topKey = new Key(lastRowFound, keys.get(0).getColumnFamily());
        topValue = encodeRow(keys, values);
    }

    @Override
    public boolean hasTop() {
        return !finished && (topKey != null || getSource().hasTop());
    }

    @Override
    public void next() throws IOException {
        skipRow();
        prepKeys();
    }

    protected SortedKeyValueIterator<Key, Value> getSource() {
        if (source == null)
            throw new IllegalStateException("getting null source");
        return source;
    }

    protected void setSource(SortedKeyValueIterator<Key, Value> source) {
        this.source = source;
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
                else {
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
            if (range.beforeStartKey(getSource().getTopKey()))
                skipRow();
        }

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
        HashMap<String, String> namedOptions = new HashMap<String, String>();
        namedOptions.put(NUM_SCANS_STRING_NAME, "Number of scans to try before seeking [10]");
        namedOptions.put(NUM_KEYS_STRING_NAME, "Number of entries to keep per row [25]");
        return new IteratorOptions(name, desc, namedOptions, null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        try {
            String o = options.get(NUM_SCANS_STRING_NAME);
            if (o != null)
                Integer.parseInt(o);

            String nStr = options.get(NUM_KEYS_STRING_NAME);
            if (nStr != null)
                Integer.parseInt(nStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("bad integer " + NUM_SCANS_STRING_NAME + ":" + options.get(NUM_SCANS_STRING_NAME), e);
        }
        return true;
    }

}
