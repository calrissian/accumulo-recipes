package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.shard.ShardBuilder;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.utils.NodeUtils;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.*;

import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;
import static org.calrissian.mango.criteria.utils.NodeUtils.isRangeLeaf;

public class EventGlobalIndexVisitor implements GlobalIndexVisitor {

  private static TypeRegistry<String> registry = ACCUMULO_TYPES;

  private Date start;
  private Date end;
  private BatchScanner indexScanner;
  private ShardBuilder shardBuilder;

  private Set<String> shards = new HashSet<String>();
  private Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();

  private Set<Leaf> leaves = new HashSet<Leaf>();

  public EventGlobalIndexVisitor(Date start, Date end, BatchScanner indexScanner, ShardBuilder shardBuilder) {
    this.start = start;
    this.end = end;
    this.indexScanner = indexScanner;
    this.shardBuilder = shardBuilder;
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
  public void begin(ParentNode parentNode) {



  }

  @Override
  public void end(ParentNode parentNode) {

    Collection<Range> ranges = new ArrayList<Range>();
    for(Leaf leaf : leaves) {

      AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf)leaf;

      String alias = registry.getAlias(kvLeaf.getValue());
      String startShard = shardBuilder.buildShard(start.getTime(), 0);
      String stopShard = shardBuilder.buildShard(end.getTime(), DEFAULT_PARTITION_SIZE-1) + "\uffff";

      if(isRangeLeaf(leaf)) {
        ranges.add(
          new Range(
            new Key(INDEX_K + "_" + kvLeaf.getKey(), alias, startShard),
            new Key(INDEX_K + "_" + kvLeaf.getKey(), alias, stopShard)
        ));
      } else {

        try {
          String normVal = registry.encode(kvLeaf.getValue());
          ranges.add(
            new Range(
              new Key(INDEX_V + "_" + alias + "__" + normVal, kvLeaf.getKey(), startShard),
              new Key(INDEX_V + "_" + alias + "__" + normVal, kvLeaf.getKey(), stopShard)
            ));
        } catch (TypeEncodingException e) {
          throw new RuntimeException(e);
        }
      }

      indexScanner.setRanges(ranges);

      for(Map.Entry<Key,Value> entry : indexScanner) {

        CardinalityKey key = new EventCardinalityKey(entry.getKey());
        Long cardinality = cardinalities.get(key);
        if(cardinality == null)
          cardinality = 0l;
        cardinalities.put(key, ++cardinality);
        shards.add(entry.getKey().getColumnQualifier().toString());
      }

      indexScanner.close();
    }
  }

  @Override
  public void visit(Leaf leaf) {
    leaves.add(leaf);
  }
}
