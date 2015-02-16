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
package org.calrissian.accumulorecipes.commons.iterators;

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.decodeRow;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.encodeRow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang.StringUtils;

/**
 * Given a set of fieldNames and fieldValues encoded into the value of a single keyValue, this will filter
 * those fieldNames which are included in a given set of selectFields.
 */
public class SelectFieldsExtractor extends WrappingIterator {

    protected static final String SELECT_FIELDS = "selectFields";
    private Set<String> selectFields;

    public static void setSelectFields(IteratorSetting is, Set<String> selectFields) {
        is.addOption(SELECT_FIELDS, StringUtils.join(selectFields, NULL_BYTE));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options == null)
            throw new IllegalArgumentException(SELECT_FIELDS + " must be set for " + SelectFieldsExtractor.class.getName());

        String eventFieldsOpt = options.get(SELECT_FIELDS);
        if (eventFieldsOpt == null)
            throw new IllegalArgumentException(SELECT_FIELDS + " must be set for " + SelectFieldsExtractor.class.getName());

        selectFields = Sets.newHashSet(splitPreserveAllTokens(eventFieldsOpt, NULL_BYTE));
    }

    @Override public Value getTopValue() {

        Collection<Map.Entry<Key,Value>> keysValues = Lists.newArrayList();
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(super.getTopValue().get());
            DataInputStream dis = new DataInputStream(bais);
            dis.readInt();
            long expiration = dis.readLong();
            List<Map.Entry<Key,Value>> map = decodeRow(getTopKey(), bais);
            for(Map.Entry<Key,Value> entry : map) {
                if(selectFields.contains(extractKey(entry.getKey()))) {
                    keysValues.add(Maps.immutableEntry(entry.getKey(), entry.getValue()));
                }
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(keysValues.size());
            dos.writeLong(expiration);
            dos.flush();
            encodeRow(keysValues, baos);
            return new Value(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected String extractKey(Key key) {
        String cq =  key.getColumnQualifier().toString();
        int idx = cq.indexOf(NULL_BYTE);
        return cq.substring(0, idx);
    }
}
