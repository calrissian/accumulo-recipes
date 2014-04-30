package org.calrissian.accumulorecipes.entitystore.impl;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;

public class AccumuloEntityStore implements EntityStore {

  public static final String DEFAULT_IDX_TABLE_NAME = "entityStore_index";
  public static final String DEFAULT_SHARD_TABLE_NAME = "entityStore_shard";

  public static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

  public static final EntityShardBuilder DEFAULT_SHARD_BUILDER = new EntityShardBuilder(5);

  private EntityShardBuilder shardBuilder;
  private final Connector connector;
  private final String indexTable;
  private final String shardTable;
  private final StoreConfig config;
  private final MultiTableBatchWriter multiTableWriter;

  private static final Kryo kryo = new Kryo();

  private final NodeToJexl nodeToJexl;

  private static TypeRegistry<String> typeRegistry;

  public AccumuloEntityStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    this(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_STORE_CONFIG);
  }

  public AccumuloEntityStore(Connector connector, String indexTable, String shardTable, StoreConfig config)
          throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    checkNotNull(connector);
    checkNotNull(indexTable);
    checkNotNull(shardTable);
    checkNotNull(config);

    this.connector = connector;
    this.indexTable = indexTable;
    this.shardTable = shardTable;
    this.typeRegistry = ACCUMULO_TYPES;
    this.config = config;
    this.shardBuilder = DEFAULT_SHARD_BUILDER;

    this.nodeToJexl = new NodeToJexl();



    if(!connector.tableOperations().exists(this.indexTable)) {
      connector.tableOperations().create(this.indexTable);
    }
    if(!connector.tableOperations().exists(this.shardTable)) {
      connector.tableOperations().create(this.shardTable);
    }

    initializeKryo(kryo);
    this.multiTableWriter = connector.createMultiTableBatchWriter(config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
  }

  @Override
  public void shutdown() throws MutationsRejectedException {
    multiTableWriter.close();
  }

  public void setShardBuilder(EntityShardBuilder shardBuilder) {
    this.shardBuilder = shardBuilder;
  }
  @Override
  public void save(Iterable<Entity> entities) {

    checkNotNull(entities);
    try {
      for(Entity entity : entities) {

        //If there are no tuples then don't write anything to the data store.
        if(entity.tuples() != null) {

          String shardId = shardBuilder.buildShard(entity.getType(), entity.getId());
          String typeId = entity.getType() + INNER_DELIM + entity.getId();

          // key
          Set<String> indexCache = new HashSet<String>();

          Mutation shardMutation = new Mutation(shardId);

          for(Tuple tuple : entity.tuples()) {

            String aliasValue = typeRegistry.getAlias(tuple.getValue()) + INNER_DELIM +
                    typeRegistry.encode(tuple.getValue());

            // forward mutation
            shardMutation.put(new Text(typeId),
                    new Text(tuple.getKey() + DELIM + aliasValue),
                    new ColumnVisibility(tuple.getVisibility()),
                    EMPTY_VALUE);

            // reverse mutation
            shardMutation.put(new Text(PREFIX_FI + DELIM + tuple.getKey()),
                    new Text(aliasValue + DELIM + typeId),
                    new ColumnVisibility(tuple.getVisibility()),
                    EMPTY_VALUE);  // forward mutation

            String[] strings = new String[] {
                    shardId,
                    tuple.getKey(),
                    typeRegistry.getAlias(tuple.getValue()),
                    typeRegistry.encode(tuple.getValue()),
                    tuple.getVisibility()
            };

            indexCache.add(join(strings, INNER_DELIM));
          }

          String[] idIndex = new String[] {
                  shardBuilder.buildShard(entity.getType(), entity.getId()),
                  "@id",
                  "string",
                  entity.getId(),
                  ""
          };

          indexCache.add(join(idIndex, INNER_DELIM));

          for(String indexCacheKey : indexCache) {

            String[] indexParts = splitPreserveAllTokens(indexCacheKey, INNER_DELIM);
            Mutation keyMutation = new Mutation(INDEX_K + "_" + indexParts[1]);
            Mutation valueMutation = new Mutation(INDEX_V + "_" + indexParts[2] + INNER_DELIM + indexParts[3]);

            keyMutation.put(new Text(indexParts[2]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), EMPTY_VALUE);
            valueMutation.put(new Text(indexParts[1]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), EMPTY_VALUE);
            multiTableWriter.getBatchWriter(indexTable).addMutation(keyMutation);
            multiTableWriter.getBatchWriter(indexTable).addMutation(valueMutation);
          }

          multiTableWriter.getBatchWriter(shardTable).addMutation(shardMutation);
        }
      }

      multiTableWriter.flush();
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public CloseableIterable<Entity> get(Iterable<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths) {
    return null;
  }

  @Override
  public CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths) {
    return null;
  }

  @Override
  public CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths) {
    return null;
  }
}
