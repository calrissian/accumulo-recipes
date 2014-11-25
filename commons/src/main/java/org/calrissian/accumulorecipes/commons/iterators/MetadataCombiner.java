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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerdeFactory;

import static com.google.common.collect.Lists.newArrayList;

public class MetadataCombiner extends Combiner {

  private static final String METADATA_SERDE_FACTORY_KEY = "metaSerdeFactory";
  protected MetadataSerDe metadataSerDe;

  @Override public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env)
      throws IOException {
    super.init(source, options, env);

    MetadataSerdeFactory metadataSerdeFactory = getFactory(options);
    if(metadataSerdeFactory == null)
      throw new RuntimeException("Metadata SerDe Factory failed to be initialized");
    else
      metadataSerDe = metadataSerdeFactory.create();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    MetadataCombiner copy = (MetadataCombiner) super.deepCopy(env);
    copy.metadataSerDe = metadataSerDe;
    return copy;
  }


  @Override
  public Value reduce(Key key, Iterator<Value> iter) {


    List<Map<String,Object>> activeList = newArrayList();
    while(iter.hasNext()) {
      Value value = iter.next();
      Collection<Map<String,Object>> metaList = metadataSerDe.deserialize(value.get());
      activeList.addAll(metaList);
    }

    return new Value(metadataSerDe.serialize(activeList));
  }

  public static final void setMetadataSerdeFactory(IteratorSetting setting, Class<? extends MetadataSerdeFactory> factoryClazz) {
    setting.addOption(METADATA_SERDE_FACTORY_KEY, factoryClazz.getName());
  }

  private MetadataSerdeFactory getFactory(Map<String,String> options) {
    String factoryClazz = options.get(METADATA_SERDE_FACTORY_KEY);
    try {
      Class clazz = Class.forName(factoryClazz);
      return (MetadataSerdeFactory) clazz.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }
}
