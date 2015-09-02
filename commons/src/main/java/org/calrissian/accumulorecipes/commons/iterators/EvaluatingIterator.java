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

import static java.lang.Long.parseLong;
import static org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter.shouldExpire;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.decodeRowSimple;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields.FieldValue;
import org.calrissian.accumulorecipes.commons.iterators.support.QueryEvaluator;

public class EvaluatingIterator extends AbstractEvaluatingIterator {

    public static final String AUTHS = "auths";

    LRUMap visibilityMap = new LRUMap();

    public EvaluatingIterator() {
        super();
    }

    public EvaluatingIterator(AbstractEvaluatingIterator other, IteratorEnvironment env) {
        super(other, env);
    }

    @Override public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
     }

  @Override
  public QueryEvaluator getQueryEvaluator(String expression) throws ParseException  {
    return new QueryEvaluator(expression);
  }

  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new EvaluatingIterator(this, env);
    }

    @Override
    public PartialKey getKeyComparator() {
        return PartialKey.ROW_COLFAM;
    }

    @Override
    public Key getReturnKey(Key k) {
        // If we were using column visibility, then we would get the merged visibility here and use it in the key.
        // Remove the COLQ from the key and use the combined visibility
        Key r = new Key(k.getRowData().getBackingArray(), k.getColumnFamilyData().getBackingArray(), EMPTY_BYTE, k.getColumnVisibility().getBytes(),
            k.getTimestamp(), k.isDeleted(), false);
        return r;
    }

    @Override
    public void fillMap(EventFields event, Key key, Value value) {
        // If we were using column visibility, we would have to merge them here.

        // Pull the datatype from the colf in case we need to do anything datatype specific.
        //    String colf = key.getColumnFamily().toString();
        //
        //    if(colf.indexOf(NULL_BYTE) > -1) {
        //
        //    }
        //
        // For the partitioned table, the field name and field values are encoded in to the value of the document keyValue. Once decoded,
        // each field name and field value are stored in the column qualifier
        // separated by a \0.

        List<Map.Entry<Key,Value>> entryList = Collections.emptyList();
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
            DataInputStream dis = new DataInputStream(bais);
            dis.readInt();
            dis.readLong(); // because we have expiration as the first byte
            entryList = decodeRowSimple(key, bais);
            bais.close();
        } catch (Exception e) {
            /**
             * It's possible there is no content encoded into the value, if this is the case, adding no fields to the
             * event will cause it to be skipped by the {@link AbstractEvaluatingIterator}
             */
        }

        try {
              for(Map.Entry<Key,Value> kv : entryList) {

                long expiration = parseLong(kv.getKey().getColumnFamily().toString());
                if(!shouldExpire(expiration, key.getTimestamp())) {
                    String colq = kv.getKey().getColumnQualifier().toString();
                    int idx = colq.indexOf(NULL_BYTE);
                    String fieldName = colq.substring(0, idx);
                    String fieldValue = colq.substring(idx + 1);
                    event.put(fieldName, new FieldValue(getColumnVisibility(key), fieldValue.getBytes(), kv.getValue().get()));
                }
              }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param key
     * @return
     */
    public ColumnVisibility getColumnVisibility(Key key) {
        ColumnVisibility result = (ColumnVisibility) visibilityMap.get(key.getColumnVisibility());
        if (result != null)
            return result;
        result = new ColumnVisibility(key.getColumnVisibility().getBytes());
        visibilityMap.put(key.getColumnVisibility(), result);
        return result;
    }

    /**
     * Don't accept this key if the colf starts with 'fi'
     */
    @Override
    public boolean isKeyAccepted(Key key) throws IOException {
        if (key.getColumnFamily().toString().startsWith("fi")) {
            Key copy = new Key(key.getRow(), new Text("fi\01"));
            Collection<ByteSequence> columnFamilies = Collections.emptyList();
            this.iterator.seek(new Range(copy, copy), columnFamilies, true);
            if (this.iterator.hasTop())
                return isKeyAccepted(this.iterator.getTopKey());
            return true;
        }
        return true;
    }

}
