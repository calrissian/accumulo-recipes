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
package org.calrissian.accumulorecipes.commons.transform;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.TupleStore;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeDecodingException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator.decodeRow;
import static org.calrissian.accumulorecipes.commons.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.commons.support.Constants.INNER_DELIM;

public abstract class KeyToTupleCollectionWholeColFXform<V extends TupleStore> implements Function<Map.Entry<Key, Value>, V> {


    private Set<String> selectFields;
    private Kryo kryo;
    private TypeRegistry<String> typeRegistry;

    public KeyToTupleCollectionWholeColFXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields) {
        this.selectFields = selectFields;
        this.kryo = kryo;
        this.typeRegistry = typeRegistry;
    }

    protected Set<String> getSelectFields() {
        return selectFields;
    }

    protected Kryo getKryo() {
        return kryo;
    }


    @Override
    public V apply(Map.Entry<Key, Value> keyValueEntry) {
        try {
            Map<Key, Value> keyValues = decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
            V entry = null;

            for (Map.Entry<Key, Value> curEntry : keyValues.entrySet()) {
                if (entry == null)
                    entry = buildEntryFromKey(curEntry.getKey());
                String[] colQParts = splitPreserveAllTokens(curEntry.getKey().getColumnQualifier().toString(), DELIM);
                String[] aliasValue = splitPreserveAllTokens(colQParts[1], INNER_DELIM);
                String visibility = curEntry.getKey().getColumnVisibility().toString();
                try {
                    entry.put(new Tuple(colQParts[0], typeRegistry.decode(aliasValue[0], aliasValue[1]), visibility));
                } catch (TypeDecodingException e) {
                    throw new RuntimeException(e);
                }
            }

            return entry;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract V buildEntryFromKey(Key k);

};