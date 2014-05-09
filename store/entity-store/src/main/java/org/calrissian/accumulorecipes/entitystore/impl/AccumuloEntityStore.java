package org.calrissian.accumulorecipes.entitystore.impl;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.EventFieldsFilteringIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.union;
import static java.util.Collections.singleton;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;
import static org.calrissian.mango.collect.CloseableIterables.transform;

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

        //If there are no getTuples then don't write anything to the data store.
        if(entity.getTuples() != null) {

          String shardId = shardBuilder.buildShard(entity.getType(), entity.getId());
          String typeId = entity.getType() + INNER_DELIM + entity.getId();

          // key
          Set<String> indexCache = new HashSet<String>();

          Mutation shardMutation = new Mutation(shardId);

          for(Tuple tuple : entity.getTuples()) {

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
    checkNotNull(typesAndIds);
    checkNotNull(auths);
    try {

      BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), config.getMaxQueryThreads());

      Collection<Range> ranges = new LinkedList<Range>();
      for(EntityIndex curIndex : typesAndIds) {
        String shardId = shardBuilder.buildShard(curIndex.getType(), curIndex.getId());
        ranges.add(new Range(new Key(shardId, curIndex.getType() + INNER_DELIM + curIndex.getId()),
                new Key(shardId, curIndex.getType() + INNER_DELIM + curIndex.getId() + DELIM)));
      }

      scanner.setRanges(ranges);

      IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
      scanner.addScanIterator(iteratorSetting);

      if(selectFields != null && selectFields.size() > 0) {
        iteratorSetting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
        EventFieldsFilteringIterator.setSelectFields(iteratorSetting, selectFields);
        scanner.addScanIterator(iteratorSetting);
      }

      return transform(closeableIterable(scanner), new WholeColFXform(selectFields));
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths) {
    checkNotNull(types);
    checkNotNull(auths);
    try {

      BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), DEFAULT_STORE_CONFIG.getMaxQueryThreads());

      Collection<Range> ranges = new LinkedList<Range>();
      for(String type : types) {
        Set<Text> shards = shardBuilder.buildShardsForTypes(singleton(type));
        for(Text shard : shards)
          ranges.add(prefix(shard.toString(), type));
      }

      scanner.setRanges(ranges);

      IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
      scanner.addScanIterator(iteratorSetting);

      if(selectFields != null && selectFields.size() > 0) {
        iteratorSetting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
        EventFieldsFilteringIterator.setSelectFields(iteratorSetting, selectFields);
        scanner.addScanIterator(iteratorSetting);
      }

      return transform(closeableIterable(scanner), new WholeColFXform(selectFields));
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths) {

    QueryOptimizer optimizer = new QueryOptimizer(query);

    String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());
    Set<Text> shards = shardBuilder.buildShardsForTypes(types);

    try {
      BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), config.getMaxQueryThreads());

      Collection<Range> ranges = new HashSet<Range>();
      for(Text shard : shards)
        ranges.add(new Range(shard));

      scanner.setRanges(ranges);

      IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
      setting.addOption(BooleanLogicIterator.QUERY_OPTION, jexl);
      setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

      scanner.addScanIterator(setting);

      if(selectFields != null && selectFields.size() > 0) {
        setting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
        EventFieldsFilteringIterator.setSelectFields(setting, union(selectFields, optimizer.getKeysInQuery()));
        scanner.addScanIterator(setting);
      }

      return transform(closeableIterable(scanner), new QueryXform(selectFields));

    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(Iterable<EntityIndex> typesAndIds, Auths auths) {
    throw new NotImplementedException();
  }

  public static class QueryXform extends KeyToTupleCollectionQueryXform<Entity> {

    public QueryXform(Set<String> selectFields) {
      super(kryo, typeRegistry, selectFields);
    }

    @Override
    protected Entity buildTupleCollectionFromKey(Key k) {
      String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), INNER_DELIM);
      return new Entity(typeId[0], typeId[1]);
    }
  }

  public static class WholeColFXform extends KeyToTupleCollectionWholeColFXform<Entity> {
    public WholeColFXform(Set<String> selectFields) {
      super(kryo, typeRegistry, selectFields);
    }

    @Override
    protected Entity buildEntryFromKey(Key k) {
      String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), INNER_DELIM);
      return new Entity(typeId[0], typeId[1]);
    }

  }

}
