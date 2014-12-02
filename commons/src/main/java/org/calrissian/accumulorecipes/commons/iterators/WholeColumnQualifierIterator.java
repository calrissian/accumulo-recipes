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
package org.calrissian.accumulorecipes.commons.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import static com.google.common.collect.Maps.immutableEntry;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;

public class WholeColumnQualifierIterator implements SortedKeyValueIterator<Key, Value> {

    List<Key> keys = new ArrayList<Key>();
    List<Value> values = new ArrayList<Value>();
    private SortedKeyValueIterator<Key, Value> sourceIter;
    private Key topKey = null;
    private Value topValue = null;

    public WholeColumnQualifierIterator() {

    }

    WholeColumnQualifierIterator(SortedKeyValueIterator<Key,Value> source) {
        this.sourceIter = source;
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

    private void prepKeys() throws IOException {
        if (topKey != null)
            return;
        Text currentRow;
        Text currentColF;
        Text currentColQ;
        if (sourceIter.hasTop() == false)
            return;

        currentRow = new Text(sourceIter.getTopKey().getRow());
        currentColF = new Text(sourceIter.getTopKey().getColumnFamily());
        currentColQ = new Text(sourceIter.getTopKey().getColumnQualifier());

        keys.clear();
        values.clear();
        while (sourceIter.hasTop() && sourceIter.getTopKey().getRow().equals(currentRow) &&
            sourceIter.getTopKey().getColumnFamily().equals(currentColF) &&
            sourceIter.getTopKey().getColumnQualifier().equals(currentColQ)) {
            keys.add(new Key(sourceIter.getTopKey()));
            values.add(new Value(sourceIter.getTopValue()));
            sourceIter.next();
        }

        topKey = new Key(currentRow, currentColF);
        topValue = encodeRow(keys, values);
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (sourceIter != null)
            return new WholeColumnQualifierIterator(sourceIter.deepCopy(env));
        return new WholeColumnQualifierIterator();
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

    private String getUuid(Key k) {

        String cq = k.getColumnQualifier().toString();
        int idx = cq.lastIndexOf(ONE_BYTE);

        return cq.substring(idx+1, cq.length());
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
