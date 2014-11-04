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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.MetadataSerdeFactory;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;

import static java.lang.Math.max;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Expiration.getExpiration;
import static org.calrissian.mango.io.Serializables.fromBase64;

/**
 * Allows Accumulo to expire keys/values based on an expiration threshold encoded in a metadata map in the value.
 */
public class MetadataExpirationFilter extends ExpirationFilter {

    private static final String METADATA_SERDE_FACTORY_KEY = "metaSerdeFactory";
    public static final String METADATA_SERDE = "metadataSerDe";
    private MetadataSerDe metadataSerDe;
    private Value nextVal = new Value();

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        MetadataSerdeFactory metadataSerdeFactory = getFactory(options);
        if(metadataSerdeFactory == null)
          throw new RuntimeException("Metadata SerDe Factory failed to be initialized");
        else
          metadataSerDe = metadataSerdeFactory.create();

        if(options.containsKey(NEGATE))
          negate = Boolean.parseBoolean(options.get(NEGATE));


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

    @Override
    public Value getTopValue() {
      if (super.getSource() == null)
        throw new IllegalStateException("no source set");
      if (seenSeek == false)
        throw new IllegalStateException("never been seeked");

      return nextVal;
    }

    boolean seenSeek = false;

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      super.seek(range, columnFamilies, inclusive);
      seenSeek = true;
    }
  /**
     * Conigurator method to configure the metadata serializer/deserializer on an iterator setting.
     */
    public static final void setMetadataSerdeFactory(IteratorSetting setting, Class<? extends MetadataSerdeFactory> factoryClazz) {
      setting.addOption(METADATA_SERDE_FACTORY_KEY, factoryClazz.getName());
    }

    protected long parseExpiration(long timestamp, Value v) {
      List<Map<String, Object>> metadata = metadataSerDe.deserialize(v.get());

      List<Map<String, Object>> newMeta = metadataSerDe.deserialize(v.get());
      long max = -1;
      if(metadata != null) {
        for(Map<String,Object> entry : metadata) {

          long expiration = getExpiration(entry, -1);
          max = max(getExpiration(entry, -1), max);

          if(!shouldExpire(expiration, timestamp))
            newMeta.add(entry);
        }

        if(!shouldExpire(max, timestamp))
          nextVal.set(metadataSerDe.serialize(newMeta));
      }

      return max;
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
