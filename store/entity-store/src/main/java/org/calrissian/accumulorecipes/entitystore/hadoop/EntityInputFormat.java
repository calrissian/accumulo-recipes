package org.calrissian.accumulorecipes.entitystore.hadoop;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.EventFieldsFilteringIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.criteria.domain.Node;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Sets.union;
import static java.util.Arrays.asList;
import static org.apache.accumulo.core.util.format.DefaultFormatter.formatEntry;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.*;

public class EntityInputFormat extends InputFormatBase<Key, EntityWritable> {

  public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
    setInputInfo(config, username, password, DEFAULT_SHARD_TABLE_NAME, auths);
  }

  public static void setQueryInfo(Configuration config, Set<String> entityTypes, Node query, Set<String> selectFields) {
    setQueryInfo(config, entityTypes, query, selectFields, DEFAULT_SHARD_BUILDER);
  }

  public static void setQueryInfo(Configuration config, Set<String> entityTypes, Node query, Set<String> selectFields, EntityShardBuilder shardBuilder) {

    Collection<Text> shards = shardBuilder.buildShardsForTypes(entityTypes);
    Collection<Range> ranges = new LinkedList<Range>();
    for(Text shard : shards)
      ranges.add(new Range(shard));

    QueryOptimizer optimizer = new QueryOptimizer(query);
    NodeToJexl nodeToJexl = new NodeToJexl();
    String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());

    setRanges(config, ranges);

    IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
    setting.addOption(BooleanLogicIterator.QUERY_OPTION, jexl);
    setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

    addIterator(config, setting);

    if(selectFields != null && selectFields.size() > 0) {
      setting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
      EventFieldsFilteringIterator.setSelectFields(setting, union(selectFields, optimizer.getKeysInQuery()));
      addIterator(config, setting);
    }
  }

  @Override
  public RecordReader<Key, EntityWritable> createRecordReader(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {

//    final EntityWritable sharedWritable = new EntityWritable();
//    final String[] selectFields = context.getConfiguration().getStrings("selectFields");
//    final QueryXform xform = new QueryXform(selectFields != null ? new HashSet<String>(asList(selectFields)) : null);

    return new RecordReaderBase<Key, EntityWritable>() {
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
//        if (scannerIterator.hasNext()) {
//          ++numKeysRead;
//          Map.Entry<Key,Value> entry = scannerIterator.next();
//          currentK = currentKey = entry.getKey();
//          sharedWritable.set(xform.apply(entry));
//          currentV =  sharedWritable;
//
//          if (log.isTraceEnabled())
//            log.trace("Processing key/value pair: " + formatEntry(entry, true));
//          return true;
//        }
//        return false;
        return false;
      }
    };
  }
}
