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
package org.calrissian.accumulorecipes.commons.support.qfd;

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.setVisibility;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields;
import org.calrissian.accumulorecipes.commons.support.attribute.metadata.MetadataSerDe;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.AttributeStore;
import org.calrissian.mango.types.TypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KeyToAttributeStoreQueryXform<V extends AttributeStore> implements Function<Map.Entry<Key, Value>, V> {

    public static final Logger log = LoggerFactory.getLogger(KeyToAttributeStoreQueryXform.class);

    private Kryo kryo;
    private TypeRegistry<String> typeRegistry;
    private MetadataSerDe metadataSerDe;

    public KeyToAttributeStoreQueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, MetadataSerDe metadataSerDe) {
        this.kryo = kryo;
        this.typeRegistry = typeRegistry;
        this.metadataSerDe = metadataSerDe;
    }

    protected Kryo getKryo() {
        return kryo;
    }

    protected TypeRegistry<String> getTypeRegistry() {
        return typeRegistry;
    }

    @Override
    public V apply(Map.Entry<Key, Value> keyValueEntry) {
        EventFields eventFields = new EventFields();
        eventFields.read(kryo, new Input(keyValueEntry.getValue().get()), EventFields.class);
        V entry = buildAttributeCollectionFromKey(keyValueEntry.getKey());
        for (Map.Entry<String, EventFields.FieldValue> fieldValue : eventFields.entries()) {
            String[] aliasVal = splitPreserveAllTokens(new String(fieldValue.getValue().getValue()), ONE_BYTE);
            Object javaVal = typeRegistry.decode(aliasVal[0], aliasVal[1]);

            String vis = fieldValue.getValue().getVisibility().getExpression().length > 0 ? new String(fieldValue.getValue().getVisibility().getExpression()) : "";

            try {
                Map<String,String> meta = metadataSerDe.deserialize(fieldValue.getValue().getMetadata());
                Map<String,String> metadata = (meta == null ? new HashMap<String,String>() : new HashMap<String,String>(meta));
                setVisibility(metadata, vis);
                Attribute attribute = new Attribute(fieldValue.getKey(), javaVal, metadata);
                entry.put(attribute);
            } catch(Exception e) {
                log.error("There was an error deserializing the metadata for a attribute", e);
            }
        }
        return entry;
    }


    protected abstract V buildAttributeCollectionFromKey(Key k);
}
