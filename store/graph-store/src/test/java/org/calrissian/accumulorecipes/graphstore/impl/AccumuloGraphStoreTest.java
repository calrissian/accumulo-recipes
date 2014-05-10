package org.calrissian.accumulorecipes.graphstore.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class AccumuloGraphStoreTest {

  private AccumuloEntityGraphStore graphStore;
  private Connector connector;

  @Before
  public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Instance instance = new MockInstance();
    connector = instance.getConnector("root", "".getBytes());
    graphStore = new AccumuloEntityGraphStore(connector);
  }

  @Test
  public void testSave() throws TableNotFoundException {

    Entity vertex1 = new BaseEntity("vertex", "id1");
    Entity vertex2 = new BaseEntity("vertex", "id2");
    Entity edge = new EdgeEntity("edge", "edgeId", vertex1, vertex2, "label1");

    vertex1.put(new Tuple("key1", "val1", ""));
    vertex1.put(new Tuple("key2", "val2", ""));
    vertex2.put(new Tuple("key3", "val3", ""));
    vertex2.put(new Tuple("key4", "val4", ""));

    graphStore.save(Arrays.asList(new Entity[] { vertex1, edge, vertex2 }));

    Scanner scanner = connector.createScanner("entityStore_graph", new Authorizations());
    for(Map.Entry<Key,Value> entry : scanner) {
      System.out.println(entry);
    }
  }

  @Test
  public void testAdjacentEdges_withLabels() throws TableNotFoundException {

    Entity vertex1 = new BaseEntity("vertex", "id1");
    Entity vertex2 = new BaseEntity("vertex", "id2");
    Entity edge = new EdgeEntity("edge", "edgeId", vertex1, vertex2, "label1");
    edge.put(new Tuple("edgeProp1", "edgeVal1"));

    vertex1.put(new Tuple("key1", "val1", ""));
    vertex1.put(new Tuple("key2", "val2", ""));
    vertex2.put(new Tuple("key3", "val3", ""));
    vertex2.put(new Tuple("key4", "val4", ""));

    graphStore.save(Arrays.asList(new Entity[] { vertex1, edge, vertex2 }));

    CloseableIterable<Entity> results = graphStore.adjacentEdges(Arrays.asList(new Entity[] {vertex1}),
      new QueryBuilder().eq("edgeProp1", "edgeVal1").build(),
      Direction.OUT,
      singleton("label1"),
      new Auths()
    );

    assertEquals(1, size(results));

    Entity actualEdge = get(results, 0);
    assertEquals(new HashSet(edge.getTuples()), new HashSet(actualEdge.getTuples()));
    assertEquals(edge.getType(), actualEdge.getType());
    assertEquals(edge.getId(), actualEdge.getId());

    results.closeQuietly();
  }


  @Test
  public void testAdjacentEdges_noLabels() throws TableNotFoundException {

    Entity vertex1 = new BaseEntity("vertex", "id1");
    Entity vertex2 = new BaseEntity("vertex", "id2");
    Entity edge = new EdgeEntity("edge", "edgeId", vertex1, vertex2, "label1");
    edge.put(new Tuple("edgeProp1", "edgeVal1"));

    vertex1.put(new Tuple("key1", "val1", ""));
    vertex1.put(new Tuple("key2", "val2", ""));
    vertex2.put(new Tuple("key3", "val3", ""));
    vertex2.put(new Tuple("key4", "val4", ""));

    graphStore.save(Arrays.asList(new Entity[] { vertex1, edge, vertex2 }));

    CloseableIterable<Entity> results = graphStore.adjacentEdges(Arrays.asList(new Entity[] {vertex1}),
            new QueryBuilder().eq("edgeProp1", "edgeVal1").build(),
            Direction.OUT,
            new Auths()
    );

    assertEquals(1, size(results));

    Entity actualEdge = get(results, 0);
    assertEquals(new HashSet(edge.getTuples()), new HashSet(actualEdge.getTuples()));
    assertEquals(edge.getType(), actualEdge.getType());
    assertEquals(edge.getId(), actualEdge.getId());

    results.closeQuietly();
  }
}
