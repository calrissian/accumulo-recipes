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
package org.calrissian.accumulorecipes.commons.iterators.support;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.ByteArraySerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Object used to hold the fields in an event. This is a multimap because fields can be repeated.
 */
public class EventFields extends Serializer<EventFields> {

    private static boolean kryoInitialized = false;
    private static ByteArraySerializer valueSerializer = new ByteArraySerializer();
    private static DefaultSerializers.IntSerializer intSerializer = new DefaultSerializers.IntSerializer();
    private static DefaultSerializers.StringSerializer stringSerializer = new DefaultSerializers.StringSerializer();

    private Map<String, Set<FieldValue>> map = null;

    private int size = 0;
    public EventFields() {
        map = new HashMap<String,Set<FieldValue>>();
    }

    @Override public void write(Kryo kryo, Output output, EventFields eventFields) {
        // Write out the number of entries;
        intSerializer.write(kryo, output, size);
        for (Entry<String, Set<FieldValue>> entry : map.entrySet()) {
            for(FieldValue fieldValue : entry.getValue()) {
                // Write the fields in the value
                stringSerializer.write(kryo, output, entry.getKey());
                valueSerializer.write(kryo, output, fieldValue.getVisibility().getExpression().length > 0 ? fieldValue.getVisibility().flatten() : fieldValue.getVisibility().getExpression());
                valueSerializer.write(kryo, output, fieldValue.getValue());
                valueSerializer.write(kryo, output, fieldValue.getMetadata());
            }
        }

        output.flush();

    }

    @Override
    public EventFields read(Kryo kryo, Input input, Class<EventFields> eventFieldsClass) {

        // Read in the number of map entries
        int entries = intSerializer.read(kryo, input, Integer.class);
        for (int i = 0; i < entries; i++) {
            // Read in the key
            String key = stringSerializer.read(kryo, input, String.class);
            // Read in the fields in the value
            ColumnVisibility vis = new ColumnVisibility(valueSerializer.read(kryo, input, byte[].class));
            byte[] value = valueSerializer.read(kryo, input, byte[].class);
            byte[] metadata = valueSerializer.read(kryo, input, byte[].class);
            put(key, new FieldValue(vis, value, metadata));
        }

        return this;
    }

    public static synchronized void initializeKryo(Kryo kryo) {
        if (kryoInitialized)
            return;
        valueSerializer = new ByteArraySerializer();
        kryo.register(byte[].class, valueSerializer);

        kryoInitialized = true;
    }

    public int size() {
        return map.size();
    }

    public Set<Map.Entry<String, Set<FieldValue>>> entrySet() {
        return map.entrySet();
    }

    public Map<String, Set<FieldValue>> asMap() {
        return unmodifiableMap(map);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean put(String key, FieldValue value) {

        Set<FieldValue> fieldValues = map.get(key);
        if(fieldValues == null) {
            fieldValues = new HashSet<FieldValue>();
            map.put(key, fieldValues);
        }

        if(!fieldValues.contains(value))
            size++;

        return fieldValues.add(value);
    }

    public void clear() {
        map.clear();
        size = 0;
    }

    public Set<String> keys() {
        return map.keySet();
    }

    public Set<FieldValue> get(String key) {
        return map.get(key);
    }

    public Set<FieldValue> removeAll(String key) {
        Set<FieldValue> values =  map.remove(key);
        if(values != null)
            size -= values.size();
        return values;
    }

    public int getByteSize() {
        int count = 0;
        for (Entry<String, Set<FieldValue>> e : map.entrySet()) {
            for(FieldValue fieldValue : e.getValue())
                count += e.getKey().getBytes().length + fieldValue.size();
        }
        return count;
    }



    public static class FieldValue {
        ColumnVisibility visibility;
        byte[] value;
        byte[] metadata;

        public FieldValue(ColumnVisibility visibility, byte[] value, byte[] metadata) {
            super();
            this.visibility = visibility;
            this.value = value;
            this.metadata = metadata;
        }

        public ColumnVisibility getVisibility() {
            return visibility;
        }

        public void setVisibility(ColumnVisibility visibility) {
            this.visibility = visibility;
        }

        public byte[] getValue() {
            return value;
        }

        public byte[] getMetadata() {
            return metadata;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public int size() {
            return visibility.getExpression().length > 0 ? visibility.flatten().length + value.length + metadata.length : value.length + metadata.length;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            if (null != visibility)
                buf.append(" visibility: ").append(new String(visibility.flatten()));
            if (null != value)
                buf.append(" value size: ").append(value.length);
            if (null != value)
                buf.append(" value: ").append(new String(value));
            if (null != metadata)
                buf.append(" value size: ").append(metadata.length);
            if (null != metadata)
                buf.append(" value: ").append(new String(metadata));

            return buf.toString();
        }

    }

}
