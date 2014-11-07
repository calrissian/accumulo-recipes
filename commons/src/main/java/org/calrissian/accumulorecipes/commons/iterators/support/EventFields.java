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

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields.FieldValue;

/**
 * Object used to hold the fields in an event. This is a multimap because fields can be repeated.
 */
public class EventFields extends Serializer<EventFields> implements SetMultimap<String, FieldValue> {

    private static boolean kryoInitialized = false;
    private static DefaultArraySerializers.ByteArraySerializer valueSerializer = new DefaultArraySerializers.ByteArraySerializer();
    private static DefaultSerializers.IntSerializer intSerializer = new DefaultSerializers.IntSerializer();
    private static DefaultSerializers.StringSerializer stringSerializer = new DefaultSerializers.StringSerializer();

    private Multimap<String, FieldValue> map = null;

    public EventFields() {
        map = HashMultimap.create();
    }

    public EventFields(Kryo kryo, Input input) {

      this();
      if(!kryoInitialized)
        EventFields.initializeKryo(kryo);
    }

  @Override public void write(Kryo kryo, Output output, EventFields eventFields) {
    // Write out the number of entries;
    intSerializer.write(kryo, output, map.size());
    for (Entry<String, FieldValue> entry : map.entries()) {
      // Write the key
      stringSerializer.write(kryo, output, entry.getKey());
      // Write the fields in the value

      valueSerializer.write(kryo, output, entry.getValue().getVisibility().getExpression().length > 0 ? entry.getValue().getVisibility().flatten() : entry.getValue().getVisibility().getExpression());
      valueSerializer.write(kryo, output, entry.getValue().getValue());
      valueSerializer.write(kryo, output, entry.getValue().getMetadata());
    }

    output.flush();

  }

  @Override public EventFields read(Kryo kryo, Input input, Class<EventFields> eventFieldsClass) {

    // Read in the number of map entries
    int entries = intSerializer.read(kryo, input, Integer.class);
    for (int i = 0; i < entries; i++) {
      // Read in the key
      String key = stringSerializer.read(kryo, input, String.class);
      // Read in the fields in the value
      ColumnVisibility vis = new ColumnVisibility(valueSerializer.read(kryo, input, byte[].class));
      byte[] value = valueSerializer.read(kryo, input, byte[].class);
      byte[] metadata = valueSerializer.read(kryo, input, byte[].class);
      map.put(key, new FieldValue(vis, value, metadata));
    }

    return this;
  }

  public static synchronized void initializeKryo(Kryo kryo) {
        if (kryoInitialized)
            return;
        valueSerializer = new DefaultArraySerializers.ByteArraySerializer();
        kryo.register(byte[].class, valueSerializer);

        kryoInitialized = true;
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    public boolean containsEntry(Object key, Object value) {
        return map.containsEntry(key, value);
    }

    public boolean put(String key, FieldValue value) {
        return map.put(key, value);
    }

    public boolean remove(Object key, Object value) {
        return map.remove(key, value);
    }

    public boolean putAll(String key, Iterable<? extends FieldValue> values) {
        return map.putAll(key, values);
    }

    public boolean putAll(Multimap<? extends String, ? extends FieldValue> multimap) {
        return map.putAll(multimap);
    }

    public void clear() {
        map.clear();
    }

    public Set<String> keySet() {
        return map.keySet();
    }

    public Multiset<String> keys() {
        return map.keys();
    }

    public Collection<FieldValue> values() {
        return map.values();
    }

    public Set<FieldValue> get(String key) {
        return (Set<FieldValue>) map.get(key);
    }

    public Set<FieldValue> removeAll(Object key) {
        return (Set<FieldValue>) map.removeAll(key);
    }

    public Set<FieldValue> replaceValues(String key, Iterable<? extends FieldValue> values) {
        return (Set<FieldValue>) map.replaceValues(key, values);
    }

    public Set<Entry<String, FieldValue>> entries() {
        return (Set<Entry<String, FieldValue>>) map.entries();
    }

    public Map<String, Collection<FieldValue>> asMap() {
        return map.asMap();
    }

    public int getByteSize() {
        int count = 0;
        for (Entry<String, FieldValue> e : map.entries()) {
            count += e.getKey().getBytes().length + e.getValue().size();
        }
        return count;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (Entry<String, FieldValue> entry : map.entries()) {
            buf.append("\tkey: ").append(entry.getKey()).append(" -> ").append(entry.getValue().toString()).append("\n");
        }
        return buf.toString();
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
