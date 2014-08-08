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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;

import java.io.IOException;
import java.util.Map;

import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Expiration.getExpiration;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;

/**
 * Allows Accumulo to expire keys/values based on an expiration threshold encoded in a metadata map in the value.
 */
public class MetadataExpirationFilter extends ExpirationFilter {

    public static final String METADATA_SERDE = "metadataSerDe";
    private MetadataSerDe metadataSerDe;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        try {
            metadataSerDe = fromBase64(options.get(METADATA_SERDE).getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        MetadataExpirationFilter copy = (MetadataExpirationFilter) super.deepCopy(env);
        copy.metadataSerDe = metadataSerDe;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("metadataExpirationFilter");
        io.addNamedOption(METADATA_SERDE, "The metadata serializer/deserializer to use. This must match the SerDe that was use to encode the metadata");
        io.setDescription("MetadataExpirationFilter removes entries with timestamps more than <ttl> milliseconds old & timestamps newer than currentTime. ttl is determined by an expiration field in metadata encoded in the value");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        super.validateOptions(options);
        try {
            fromBase64(options.get(METADATA_SERDE).getBytes());
        } catch (Exception e) {
            return false;
        }
        return true;
    }


    /**
     * Conigurator method to configure the metadata serializer/deserializer on an iterator setting.
     */
    public static void setMetadataSerde(IteratorSetting setting, MetadataSerDe metadataSerDe) {
        try {
            setting.addOption(METADATA_SERDE, new String(toBase64(metadataSerDe)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected long parseExpiration(Value v) {
        Map<String, Object> metadata = metadataSerDe.deserialize(v.get());
        if(metadata != null)
            return getExpiration(metadata, -1);
        return -1;
    }

}
