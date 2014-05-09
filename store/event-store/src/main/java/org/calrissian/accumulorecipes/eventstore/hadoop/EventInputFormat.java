package org.calrissian.accumulorecipes.eventstore.hadoop;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.calrissian.accumulorecipes.commons.hadoop.StoreEntryWritable;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.EventFieldsFilteringIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.shard.ShardBuilder;
import org.calrissian.mango.criteria.domain.Node;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Sets.union;
import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.*;

public class EventInputFormat extends InputFormatBase<Key, StoreEntryWritable> {

  public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
    setInputInfo(config, username, password, DEFAULT_SHARD_TABLE_NAME, auths);
  }

  public static void setQueryInfo(Configuration config, Date start, Date end, Node query, Set<String> selectFields) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    setQueryInfo(config, start, end, query, selectFields, DEFAULT_SHARD_BUILDER);
  }

  public static void setQueryInfo(Configuration config, Date start, Date end, Node query, Set<String> selectFields, ShardBuilder shardBuilder) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    try {
      validateOptions(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Instance instance = getInstance(config);
    Connector connector = instance.getConnector(getUsername(config), getPassword(config));
    BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getAuthorizations(config), 5);
    GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, scanner, shardBuilder);
    QueryOptimizer optimizer = new QueryOptimizer(query, globalIndexVisitor);
    NodeToJexl nodeToJexl = new NodeToJexl();
    String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());

    Collection<Range> ranges = new ArrayList<Range>();
    for(String shard : optimizer.getShards())
      ranges.add(new Range(shard));

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
  public RecordReader<Key, StoreEntryWritable> createRecordReader(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {

    final StoreEntryWritable sharedWritable = new StoreEntryWritable();
    final String[] selectFields = context.getConfiguration().getStrings("selectFields");
    final QueryXform xform = new QueryXform(selectFields != null ? new HashSet<String>(asList(selectFields)) : null);

    return new RecordReaderBase<Key, StoreEntryWritable>() {
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Map.Entry<Key,Value> entry = scannerIterator.next();
          currentK = currentKey = entry.getKey();
          sharedWritable.set(xform.apply(entry));
          currentV =  sharedWritable;

          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    };
  }
}
