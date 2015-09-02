/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.util;

import static com.google.common.collect.Maps.immutableEntry;
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
import org.apache.accumulo.core.data.Value;

public class RowEncoderUtil {

    private static final byte[] empty = new byte[]{};

    public static final List<Map.Entry<Key,Value>> decodeRow(Key rowKey, ByteArrayInputStream in) throws IOException {
        List<Map.Entry<Key,Value>> map = new ArrayList<Map.Entry<Key, Value>>();
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

    public static final List<Map.Entry<Key,Value>> decodeRowSimple(Key rowKey, ByteArrayInputStream in) throws IOException {
        List<Map.Entry<Key,Value>> map = new ArrayList<Map.Entry<Key, Value>>();
        DataInputStream din = new DataInputStream(in);
        int numKeys = din.readInt();

        for (int i = 0; i < numKeys; i++) {
            byte[] cf;
            byte[] cq;
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
            // read the value
            {
                int len = din.readInt();
                valBytes = new byte[len];
                din.read(valBytes);
            }
            map.add(immutableEntry(new Key(rowKey.getRowData().toArray(), cf, cq, empty, -1, false, false), new Value(valBytes, false)));
        }
        return map;
    }

    // decode a bunch of key value pairs that have been encoded into a single value
    public static final List<Map.Entry<Key,Value>> decodeRowSimple(Key rowKey, Value rowValue) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(rowValue.get());
        return decodeRowSimple(rowKey, in);
    }


    // decode a bunch of key value pairs that have been encoded into a single value
    public static final List<Map.Entry<Key,Value>> decodeRow(Key rowKey, Value rowValue) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(rowValue.get());
        return decodeRow(rowKey, in);
    }

    /**
     * Uses the given byte array output stream to encode row data
     * @param keyValueCollection
     * @param out
     * @throws IOException
     */
    public static final void encodeRow(Collection<Map.Entry<Key,Value>> keyValueCollection, ByteArrayOutputStream out) throws IOException {
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeInt(keyValueCollection.size());

        for(Map.Entry<Key,Value> entry : keyValueCollection) {
            Key k = entry.getKey();
            Value v = entry.getValue();
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
    }

    public static final void encodeRowSimple(Collection<Map.Entry<Key,Value>> keyValueCollection, ByteArrayOutputStream out) throws IOException {
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeInt(keyValueCollection.size());

        for(Map.Entry<Key,Value> entry : keyValueCollection) {
            Key k = entry.getKey();
            Value v = entry.getValue();
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
            // write the value
            byte[] valBytes = v.get();
            dout.writeInt(valBytes.length);
            dout.write(valBytes);
        }
    }


    // take a stream of keys and values and output a value that encodes everything but their row
    // keys and values must be paired one for one
    public static final Value encodeRowSimple(Collection<Map.Entry<Key,Value>> keyValueCollection) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        encodeRowSimple(keyValueCollection, out);
        return new Value(out.toByteArray());
    }


    // take a stream of keys and values and output a value that encodes everything but their row
    // keys and values must be paired one for one
    public static final Value encodeRow(Collection<Map.Entry<Key,Value>> keyValueCollection) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        encodeRow(keyValueCollection, out);
        return new Value(out.toByteArray());
    }
}
