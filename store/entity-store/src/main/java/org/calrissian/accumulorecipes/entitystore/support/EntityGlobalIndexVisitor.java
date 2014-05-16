package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.*;

import static java.lang.Long.parseLong;
import static org.apache.accumulo.core.data.Range.exact;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.mango.criteria.support.NodeUtils.isRangeLeaf;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EntityGlobalIndexVisitor implements GlobalIndexVisitor {

  private static TypeRegistry<String> registry = LEXI_TYPES;

  private BatchScanner indexScanner;
  private EntityShardBuilder shardBuilder;

  private Set<String> shards = new HashSet<String>();
  private Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();

  private Set<String> types;

  private Set<Leaf> leaves = new HashSet<Leaf>();

  public EntityGlobalIndexVisitor(BatchScanner indexScanner, EntityShardBuilder shardBuilder, Set<String> types) {
    this.indexScanner = indexScanner;
    this.shardBuilder = shardBuilder;
    this.types = types;
  }

  @Override
  public Map<CardinalityKey, Long> getCardinalities() {
    return cardinalities;
  }

  @Override
  public Set<String> getShards() {
    return shards;
  }

  @Override
  public void begin(ParentNode parentNode) {}

  @Override
  public void end(ParentNode parentNode) {  }

  public void exec() {
    Collection<Range> ranges = new ArrayList<Range>();
    for(Leaf leaf : leaves) {

      AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

      String alias = registry.getAlias(kvLeaf.getValue());

      for (String type : types) {
        if (isRangeLeaf(leaf) || leaf instanceof HasLeaf || leaf instanceof HasNotLeaf) {
          if (alias != null)
            ranges.add(exact(type + "_" + INDEX_K + "_" + kvLeaf.getKey(), alias));
          else
            ranges.add(exact(type + "_" + INDEX_K + "_" + kvLeaf.getKey()));
        } else {
          try {
            String normVal = registry.encode(kvLeaf.getValue());
            ranges.add(exact(type + "_" + INDEX_V + "_" + alias + "__" + normVal, kvLeaf.getKey()));
          } catch (TypeEncodingException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    indexScanner.setRanges(ranges);

    for(Map.Entry<Key,Value> entry : indexScanner) {

      CardinalityKey key = new EntityCardinalityKey(entry.getKey());
      Long cardinality = cardinalities.get(key);
      if(cardinality == null)
        cardinality = 0l;
      long newCardinality = parseLong(new String(entry.getValue().get()));
      cardinalities.put(key, cardinality + newCardinality);
      shards.add(entry.getKey().getColumnQualifier().toString());
    }

    indexScanner.close();
  }

  @Override
  public void visit(Leaf leaf) {
    leaves.add(leaf);
  }
}
