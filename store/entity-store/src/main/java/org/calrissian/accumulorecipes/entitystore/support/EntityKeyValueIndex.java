package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;

public class EntityKeyValueIndex implements KeyValueIndex<Entity> {

  private ShardBuilder<Entity> shardBuilder;
  private TypeRegistry<String> typeRegistry;

  private BatchWriter writer;

  public EntityKeyValueIndex(Connector connector, String indexTable, ShardBuilder<Entity> shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry) throws TableNotFoundException {
    this.shardBuilder = shardBuilder;
    this.typeRegistry = typeRegistry;

    writer = connector.createBatchWriter(indexTable, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
  }

  @Override
  public void indexKeyValues(Iterable<Entity> items) {

    Map<String, Long> indexCache = new HashMap<String, Long>();

    for(Entity entity : items) {
      String shardId = shardBuilder.buildShard(entity);
      for (Tuple tuple : entity.getTuples()) {
        try {
          String[] strings = new String[]{
                  entity.getType(),
                  shardId,
                  tuple.getKey(),
                  typeRegistry.getAlias(tuple.getValue()),
                  typeRegistry.encode(tuple.getValue()),
                  tuple.getVisibility(),
          };

          String cacheKey = join(strings, INNER_DELIM);
          Long count = indexCache.get(cacheKey);
          if (count == null)
            count = 0l;
          indexCache.put(cacheKey, ++count);

        } catch (TypeEncodingException e) {
          throw new RuntimeException(e);
        }
      }
    }

    for (Map.Entry<String, Long> indexCacheKey : indexCache.entrySet()) {

      String[] indexParts = splitPreserveAllTokens(indexCacheKey.getKey(), INNER_DELIM);

      String entityType = indexParts[0];
      String shard = indexParts[1];
      String key = indexParts[2];
      String alias = indexParts[3];
      String normalizedVal = indexParts[4];
      String vis = indexParts[5];

      Mutation keyMutation = new Mutation(entityType + "_" + INDEX_K + "_" + key);
      Mutation valueMutation = new Mutation(entityType + "_" + INDEX_V + "_" + alias + "__" + normalizedVal);

      Value value = new Value(Long.toString(indexCacheKey.getValue()).getBytes());
      keyMutation.put(new Text(alias), new Text(shard), new ColumnVisibility(vis), value);
      valueMutation.put(new Text(key), new Text(shard), new ColumnVisibility(vis), value);
      try {
        writer.addMutation(keyMutation);
        writer.addMutation(valueMutation);
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void commit() throws Exception {
    writer.flush();
  }
}
