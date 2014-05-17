package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_PARTITION_SIZE;
import static org.calrissian.accumulorecipes.test.AccumuloTestUtils.dumpTable;
import static org.junit.Assert.assertEquals;

public class EntityGlobalIndexVisitorTest {

  @Test
  public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

    Instance instance = new MockInstance();
    Connector connector = instance.getConnector("root", "".getBytes());
    EntityStore entityStore = new AccumuloEntityStore(connector);

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1"));
    entity.put(new Tuple("key2", "val2"));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("key1", "val1"));
    entity2.put(new Tuple("key2", "val2"));
    entity2.put(new Tuple("key3", true));

    entityStore.save(asList(new Entity[]{entity, entity2}));

    dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

    Node node = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").eq("key3", true).end().build();

    BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, new Authorizations(), 2);
    GlobalIndexVisitor visitor = new EntityGlobalIndexVisitor(scanner,
            new EntityShardBuilder(DEFAULT_PARTITION_SIZE),
            Collections.singleton("type"));

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
