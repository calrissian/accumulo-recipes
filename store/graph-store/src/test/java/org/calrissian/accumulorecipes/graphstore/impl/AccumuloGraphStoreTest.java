package org.calrissian.accumulorecipes.graphstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.mango.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.calrissian.accumulorecipes.graphstore.model.Edge.*;

public class AccumuloGraphStoreTest {

  private AccumuloEntityStore graphStore;
  private Connector connector;

  @Before
  public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Instance instance = new MockInstance();
    connector = instance.getConnector("root", "".getBytes());
    graphStore = new AccumuloEntityGraphStore(connector);
  }

  @Test
  public void testSave() throws TableNotFoundException {

    Entity vertex1 = new Entity("vertex", "id1");
    Entity edge = new Entity("edge", "edgeId");
    Entity vertex2 = new Entity("vertex", "id2");

    vertex1.put(new Tuple("key1", "val1", ""));
    vertex1.put(new Tuple("key2", "val2", ""));
    edge.put(new Tuple(HEAD, new EntityRelationship(vertex1), ""));
    edge.put(new Tuple(TAIL, new EntityRelationship(vertex2), ""));
    edge.put(new Tuple(LABEL, "label1", ""));
    vertex2.put(new Tuple("key3", "val3", ""));

    vertex2.put(new Tuple("key4", "val4", ""));

    graphStore.save(Arrays.asList(new Entity[] { vertex1, edge, vertex2 }));

    Scanner scanner = connector.createScanner("entityStore_graph", new Authorizations());
    for(Map.Entry<Key,Value> entry : scanner) {
      System.out.println(entry);
    }


  }
}
