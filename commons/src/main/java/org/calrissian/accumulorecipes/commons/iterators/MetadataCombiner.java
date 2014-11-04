package org.calrissian.accumulorecipes.commons.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.MetadataSerdeFactory;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;

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

    List<Map<String,Object>> activeList = new ArrayList<>();
    while(iter.hasNext()) {
      Value value = iter.next();
      List<Map<String,Object>> metaList = metadataSerDe.deserialize(value.get());
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
    }

    return null;
  }
}
