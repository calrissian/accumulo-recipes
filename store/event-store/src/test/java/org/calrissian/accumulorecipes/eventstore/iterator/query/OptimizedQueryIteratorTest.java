package org.calrissian.accumulorecipes.eventstore.iterator.query;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.EvaluatingIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

public class OptimizedQueryIteratorTest {

  private static Logger log = LoggerFactory.getLogger(OptimizedQueryIteratorTest.class);

  Connector connector;

  @Before
  public void setUp() throws AccumuloSecurityException, AccumuloException {

    Instance instance = new MockInstance();
    connector = instance.getConnector("user", "password".getBytes());
  }


  @Test
  public void test() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

    connector.tableOperations().create("event_index");
    connector.tableOperations().create("event_shard");
    BatchWriter writer = connector.createBatchWriter("event_shard", 1000, 1000, 1);
    BatchWriter indexWriter = connector.createBatchWriter("event_index", 1000, 1000, 1);

    Mutation m = new Mutation("0");
    m.put(new Text("auuid"), new Text("key1\0string\u000100000005"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key1"), new Text("string\u000100000005\0auuid"), new ColumnVisibility("U"), new Value("".getBytes()));

    m.put(new Text("auuid"), new Text("key\0string\u000100000006"),  new ColumnVisibility("U"),new Value("".getBytes()));
    m.put(new Text("fi\0key"), new Text("string\u000100000006\0auuid"), new ColumnVisibility("U"), new Value("".getBytes()));

    m.put(new Text("uuid2"), new Text("key\0string\1value"),  new ColumnVisibility("U"),new Value("".getBytes()));
    m.put(new Text("fi\0key"), new Text("string\1value\0uuid2"),  new ColumnVisibility("U"),new Value("".getBytes()));

    m.put(new Text("uuid3"), new Text("key2\0value2"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key2"), new Text("value2\0uuid3"),  new ColumnVisibility("U"),new Value("".getBytes()));

    m.put(new Text("uuid3"), new Text("key\0value"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key"), new Text("value\0uuid3"),  new ColumnVisibility("U"),new Value("".getBytes()));
    writer.addMutation(m);

    m = new Mutation("1");
    m.put(new Text("uuid"), new Text("key\0string\1VALUE3"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key"), new Text("string\1VALUE3\0uuid"), new ColumnVisibility("U"), new Value("".getBytes()));

    m.put(new Text("uuid0"), new Text("key\0string\1value"),  new ColumnVisibility("U"),new Value("".getBytes()));
    m.put(new Text("fi\0key"), new Text("string\1value\0uuid0"), new ColumnVisibility("U"), new Value("".getBytes()));

    m.put(new Text("uuid0"), new Text("key2\0string\100000003"),  new ColumnVisibility("U"),new Value("".getBytes()));
    m.put(new Text("fi\0key2"), new Text("string\100000003\0uuid0"),  new ColumnVisibility("U"),new Value("".getBytes()));

    m.put(new Text("uuid3"), new Text("key2\0string\1value2"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key2"), new Text("string\1value2\0uuid3"),  new ColumnVisibility("U"),new Value("".getBytes()));

    m.put(new Text("uuid3"), new Text("key\0string\1value"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key"), new Text("string\1value\0uuid3"),  new ColumnVisibility("U"),new Value("".getBytes()));

    m.put(new Text("uuid4"), new Text("key4\0string\1value"), new ColumnVisibility("U"), new Value("".getBytes()));
    m.put(new Text("fi\0key4"), new Text("string\1value\0uuid4"),  new ColumnVisibility("U"),new Value("".getBytes()));
    writer.addMutation(m);
    writer.flush();

    Mutation q = new Mutation("value2");
    q.put(new Text("key2"), new Text("0"), new Value("".getBytes()));
    indexWriter.addMutation(q);

    q = new Mutation("uuid");
    q.put(new Text("\0id"), new Text("0"), new Value("".getBytes()));
    indexWriter.addMutation(q);
    indexWriter.flush();

    String query = "key1 == 'string\u000100000005'";

    Scanner indexScanner = connector.createScanner("event_shard", new Authorizations("U"));
    Scanner shardScanner = connector.createScanner("event_index", new Authorizations("U"));

    for(Map.Entry<Key,Value> entry : indexScanner)
      System.out.println(entry);
    for(Map.Entry<Key,Value> entry : shardScanner)
      System.out.println(entry);

    BatchScanner scanner = connector.createBatchScanner("event_shard", new Authorizations("U"), 1);
    scanner.setRanges(Arrays.asList(new Range[] { new Range("0"), new Range("1")}));

    IteratorSetting setting = new IteratorSetting(15, OptimizedQueryIterator.class);
    setting.addOption(EvaluatingIterator.QUERY_OPTION,  "f:between(key, 'string\u000100000008', 'string\u000100000009')");
    setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, query);
    scanner.addScanIterator(setting);

    for(Map.Entry<Key,Value> entry : scanner) {
      System.out.println(entry.getKey().getRow() + " - " + entry.getKey().getColumnFamily().toString());
    }
  }
}
