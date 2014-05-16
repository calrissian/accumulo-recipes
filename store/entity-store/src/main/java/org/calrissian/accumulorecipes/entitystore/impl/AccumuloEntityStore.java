package org.calrissian.accumulorecipes.entitystore.impl;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
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
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.model.RelationshipTypeEncoder;
import org.calrissian.accumulorecipes.entitystore.support.EntityCardinalityKey;
import org.calrissian.accumulorecipes.entitystore.support.EntityGlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.union;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singleton;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.data.Range.exact;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope.majc;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class AccumuloEntityStore implements EntityStore {

  public static final String DEFAULT_IDX_TABLE_NAME = "entityStore_index";
  public static final String DEFAULT_SHARD_TABLE_NAME = "entityStore_shard";

  public static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

  public static final EntityShardBuilder DEFAULT_SHARD_BUILDER = new EntityShardBuilder(5);

  public static final TypeRegistry<String> ENTITY_TYPES =
          new TypeRegistry<String>(LEXI_TYPES, new RelationshipTypeEncoder());

  private EntityShardBuilder shardBuilder;
  private final Connector connector;
  private final String indexTable;
  private final String shardTable;
  private final StoreConfig config;
  private final MultiTableBatchWriter multiTableWriter;

  private static final Kryo kryo = new Kryo();

  private final NodeToJexl nodeToJexl;

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
    this.config = config;
    this.shardBuilder = DEFAULT_SHARD_BUILDER;

    this.nodeToJexl = new NodeToJexl();

    if (!connector.tableOperations().exists(this.indexTable))
      connector.tableOperations().create(this.indexTable);

    if (connector.tableOperations().getIteratorSetting(this.indexTable, "cardinalities", majc) == null) {
      IteratorSetting setting = new IteratorSetting(10, "cardinalities", SummingCombiner.class);
      SummingCombiner.setCombineAllColumns(setting, true);
      SummingCombiner.setEncodingType(setting, LongCombiner.StringEncoder.class);
      connector.tableOperations().attachIterator(this.indexTable, setting, allOf(IteratorUtil.IteratorScope.class));
    }

    if (!connector.tableOperations().exists(this.shardTable))
      connector.tableOperations().create(this.shardTable);

    initializeKryo(kryo);
    this.multiTableWriter = connector.createMultiTableBatchWriter(config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
  }

  protected StoreConfig getConfig() {
    return config;
  }

  protected Connector getConnector() {
    return connector;
  }

  @Override
  public void shutdown() throws MutationsRejectedException {
    multiTableWriter.close();
  }


  public void setShardBuilder(EntityShardBuilder shardBuilder) {
    checkNotNull(shardBuilder);
    this.shardBuilder = shardBuilder;
  }

  @Override
  public void save(Iterable<Entity> entities) {

    checkNotNull(entities);
    try {
      for (Entity entity : entities) {

        //If there are no getTuples then don't write anything to the data store.
        if (entity.getTuples() != null) {

          String shardId = shardBuilder.buildShard(entity.getType(), entity.getId());
          String typeId = entity.getType() + INNER_DELIM + entity.getId();

          // key
          Map<String, Long> indexCache = new HashMap<String, Long>();

          Mutation shardMutation = new Mutation(shardId);

          for (Tuple tuple : entity.getTuples()) {

            String aliasValue = ENTITY_TYPES.getAlias(tuple.getValue()) + INNER_DELIM +
                    ENTITY_TYPES.encode(tuple.getValue());

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

            String[] strings = new String[]{
                    entity.getType(),
                    shardId,
                    tuple.getKey(),
                    ENTITY_TYPES.getAlias(tuple.getValue()),
                    ENTITY_TYPES.encode(tuple.getValue()),
                    tuple.getVisibility(),

            };

            String cacheKey = join(strings, INNER_DELIM);
            Long count = indexCache.get(cacheKey);
            if (count == null)
              count = 0l;
            indexCache.put(cacheKey, ++count);
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
  public CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths) {
    checkNotNull(typesAndIds);
    checkNotNull(auths);
    try {

      BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), config.getMaxQueryThreads());

      Collection<Range> ranges = new LinkedList<Range>();
      for (EntityIndex curIndex : typesAndIds) {
        String shardId = shardBuilder.buildShard(curIndex.getType(), curIndex.getId());
        ranges.add(exact(shardId, curIndex.getType() + INNER_DELIM + curIndex.getId()));
      }

      scanner.setRanges(ranges);

      IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
      scanner.addScanIterator(iteratorSetting);

      if (selectFields != null && selectFields.size() > 0) {
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
      for (String type : types) {
        Set<Text> shards = shardBuilder.buildShardsForTypes(singleton(type));
        for (Text shard : shards)
          ranges.add(prefix(shard.toString(), type));
      }

      scanner.setRanges(ranges);

      IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
      scanner.addScanIterator(iteratorSetting);

      if (selectFields != null && selectFields.size() > 0) {
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

    checkNotNull(types);
    checkNotNull(query);
    checkNotNull(auths);

    checkArgument(types.size() > 0);

    try {
      BatchScanner indexScanner = connector.createBatchScanner(indexTable, auths.getAuths(), config.getMaxQueryThreads());
      QueryOptimizer optimizer = new QueryOptimizer(query, new EntityGlobalIndexVisitor(indexScanner, shardBuilder, types));
      indexScanner.close();

      if(NodeUtils.isEmpty(optimizer.getOptimizedQuery()))
        return CloseableIterables.wrap(EMPTY_LIST);

      String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());
      String originalJexl = nodeToJexl.transform(query);
      Set<String> shards = optimizer.getShards();

      BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), config.getMaxQueryThreads());

      Collection<Range> ranges = new HashSet<Range>();
      for (String shard : shards)
        ranges.add(new Range(shard));

      scanner.setRanges(ranges);

      IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
      setting.addOption(BooleanLogicIterator.QUERY_OPTION, originalJexl);
      setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

      scanner.addScanIterator(setting);

      return transform(closeableIterable(scanner), new QueryXform(selectFields));

    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterable<Pair<String, String>> keys(String type, Auths auths) {

    checkNotNull(type);
    checkNotNull(auths);

    try {
      Scanner scanner = connector.createScanner(indexTable, auths.getAuths());
      IteratorSetting setting = new IteratorSetting(15, FirstEntryInRowIterator.class);
      scanner.addScanIterator(setting);
      scanner.setRange(Range.prefix(type + "_" + INDEX_K + "_"));

      return transform(wrap(scanner), new Function<Map.Entry<Key, Value>, Pair<String, String>>() {
        @Override
        public Pair<String, String> apply(Map.Entry<Key, Value> keyValueEntry) {
          EntityCardinalityKey key = new EntityCardinalityKey(keyValueEntry.getKey());
          return new Pair<String, String>(key.getKey(), key.getAlias());
        }
      });

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
      super(kryo, ENTITY_TYPES, selectFields);
    }

    @Override
    protected Entity buildTupleCollectionFromKey(Key k) {
      String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), INNER_DELIM);
      return new BaseEntity(typeId[0], typeId[1]);
    }
  }

  public static class WholeColFXform extends KeyToTupleCollectionWholeColFXform<Entity> {
    public WholeColFXform(Set<String> selectFields) {
      super(kryo, ENTITY_TYPES, selectFields);
    }

    @Override
    protected Entity buildEntryFromKey(Key k) {
      String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), INNER_DELIM);
      return new BaseEntity(typeId[0], typeId[1]);
    }

  }

}
