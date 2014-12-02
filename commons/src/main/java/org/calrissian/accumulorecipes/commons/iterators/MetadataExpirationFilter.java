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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerdeFactory;

import static org.calrissian.accumulorecipes.commons.iterators.ExpirationFilter.shouldExpire;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Expiration.getExpiration;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Timestamp.getTimestamp;

/**
 * Allows Accumulo to expire keys/values based on an expiration threshold encoded in a metadata map in the value.
 * This is an abstract class that allows the actual fetching of the timestamp from the key/value to be supplied
 * by subclasses.
 */
public class MetadataExpirationFilter extends Filter {

    private static final String METADATA_SERDE_FACTORY_KEY = "metaSerdeFactory";
    protected MetadataSerDe metadataSerDe;

    private boolean seenSeek = false;
    private boolean negate = false;

    private static final Value EMPTY_VALUE = new Value("".getBytes());

    private Collection<Map<String,Object>> curMeta;
    private Key curKey;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        MetadataSerdeFactory metadataSerdeFactory = getFactory(options);
        if (metadataSerdeFactory == null)
            throw new RuntimeException("Metadata SerDe Factory failed to be initialized");
        else
            metadataSerDe = metadataSerdeFactory.create();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        MetadataExpirationFilter copy = (MetadataExpirationFilter) super.deepCopy(env);
        copy.metadataSerDe = metadataSerDe;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("metadataExpirationFilter");
        io.addNamedOption(METADATA_SERDE_FACTORY_KEY, "The metadata serializer/deserializer to use. This must match the SerDe that was use to encode the metadata");
        io.setDescription(
            "MetadataExpirationFilter removes entries with timestamps more than <ttl> milliseconds old & timestamps newer than currentTime. ttl is determined by an expiration field in metadata encoded in the value");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        super.validateOptions(options);
        try {
            options.containsKey(METADATA_SERDE_FACTORY_KEY);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Conigurator method to configure the metadata serializer/deserializer on an iterator setting.
     */
    public static final void setMetadataSerdeFactory(IteratorSetting setting, Class<? extends MetadataSerdeFactory> factoryClazz) {
        setting.addOption(METADATA_SERDE_FACTORY_KEY, factoryClazz.getName());
    }

    private Value nextTop;

    /**
     * Iterates over the source until an acceptable key/value pair is found. The accept() method deserializes metadata
     * from bytes once and sets the deserialized list of metadata on the current instance so that we don't need to
     */
    protected void findTop() {
        nextTop = null;
        curMeta = null;

        while (nextTop == null && getSource().hasTop()) {

            increment();

            if (curMeta == null || curMeta.size() == 0) {
                if(getSource().hasTop())
                    nextTop = getSource().getTopValue();
            }
            else {
                List<Map<String,Object>> newMeta = Lists.newArrayList();
                for (Map<String,Object> entry : curMeta) {

                    long expiration = getExpiration(entry, -1);
                    long timestamp = parseTimestamp(curKey, entry);

                    if (!shouldExpire(expiration, timestamp))
                        newMeta.add(entry);
                }
                if (newMeta.size() > 0)
                    nextTop = new Value(metadataSerDe.serialize(newMeta));
                else
                    nextTop = EMPTY_VALUE;
            }

        }
    }

    protected long parseTimestamp(Key k, Map<String,Object> meta) {
        return getTimestamp(meta, -1);
    }

    /**
     * Accepts entries whose timestamps are less than currentTime - threshold.
     */
    @Override
    public boolean accept(Key k, Value v) {

        if(v.getSize() > 0) {
            curMeta = metadataSerDe.deserialize(v.get());
            curKey = k;
            // no metadata and empty metadata will not expire
            if(curMeta.size() == 0)
                return true;

            for (Map<String,Object> entry : curMeta) {
                long expiration = getExpiration(entry, -1);
                long timestamp = parseTimestamp(k, entry);

                if (!shouldExpire(expiration, timestamp))
                    return true;
            }

            return false;
        }
        return true;
    }

    /**
     * Skip over keys that need to be expired
     */
    protected void increment() {
        while (getSource().hasTop() && !getSource().getTopKey().isDeleted() &&
            (negate == accept(getSource().getTopKey(), getSource().getTopValue()))) {
            try {
                getSource().next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * Overriding seek here just so we can set our own seenSeek property
     * @param range
     * @param columnFamilies
     * @param inclusive
     * @throws IOException
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        seenSeek = true;
    }

    @Override
    public Value getTopValue() {
        if (getSource() == null)
            throw new IllegalStateException("no source set");
        if (seenSeek == false)
            throw new IllegalStateException("never been seeked");
        return nextTop;
    }

    private MetadataSerdeFactory getFactory(Map<String,String> options) {
        String factoryClazz = options.get(METADATA_SERDE_FACTORY_KEY);
        try {
            Class clazz = Class.forName(factoryClazz);
            return (MetadataSerdeFactory) clazz.newInstance();
        } catch (Exception e) {}

        return null;
    }

}
