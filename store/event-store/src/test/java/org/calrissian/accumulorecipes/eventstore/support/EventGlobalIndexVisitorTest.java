package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.test.AccumuloTestUtils.dumpTable;
import static org.junit.Assert.assertEquals;

public class EventGlobalIndexVisitorTest {

  @Test
  public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

    Instance instance = new MockInstance();
    Connector connector = instance.getConnector("root", "".getBytes());
    EventStore eventStore = new AccumuloEventStore(connector);

    StoreEntry event = new StoreEntry("id");
    event.put(new Tuple("key1", "val1"));
    event.put(new Tuple("key2", "val2"));

    StoreEntry event2 = new StoreEntry("id2");
    event2.put(new Tuple("key1", "val1"));
    event2.put(new Tuple("key2", "val2"));
    event2.put(new Tuple("key3", true));

    eventStore.save(asList(new StoreEntry[]{event, event2}));

    dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

    Node node = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").eq("key3", true).end().build();

    BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, new Authorizations(), 2);
    GlobalIndexVisitor visitor = new EventGlobalIndexVisitor(new Date(0), new Date(), scanner,
            new HourlyShardBuilder(Constants.DEFAULT_PARTITION_SIZE));

    node.accept(visitor);
    visitor.exec();


    assertEquals(3, visitor.getCardinalities().size());
    for(Map.Entry<CardinalityKey, Long> entry : visitor.getCardinalities().entrySet()) {
      if(entry.getKey().getKey().equals("key1"))
        assertEquals(2l, (long)entry.getValue());
      else if(entry.getKey().getKey().equals("key2"))
        assertEquals(2l, (long)entry.getValue());
      else if(entry.getKey().getKey().equals("key3"))
        assertEquals(1l, (long)entry.getValue());
      else
        throw new RuntimeException("Unexpected key in cardinalities");
    }
  }
}
