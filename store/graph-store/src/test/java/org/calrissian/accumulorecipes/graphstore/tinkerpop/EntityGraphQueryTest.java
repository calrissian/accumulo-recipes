package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.impl.AccumuloEntityGraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

public class EntityGraphQueryTest {


  AccumuloEntityGraphStore entityGraphStore;
  BlueprintsGraphStore graph;
  Connector connector;
  GraphQuery query;

  Entity vertex1 = new BaseEntity("vertexType1", "id1");
  Entity vertex2 = new BaseEntity("vertexType2", "id2");
  Entity edge = new EdgeEntity("edgeType1", "edgeId", vertex1, "", vertex2, "", "label1");

  @Before
  public void start() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    Instance instance = new MockInstance();
    connector = instance.getConnector("root", "".getBytes());
    entityGraphStore = new AccumuloEntityGraphStore(connector);
    graph = new BlueprintsGraphStore(entityGraphStore, newHashSet("vertexType1", "vertexType2"),
            newHashSet("edgeType1", "edgeType2"),
            new Auths("U,ADMIN"));

    vertex1.put(new Tuple("key1", "val1", "U"));
    vertex1.put(new Tuple("key2", "val2", "U"));
    vertex2.put(new Tuple("key3", "val3", "U"));
    vertex2.put(new Tuple("key4", "val4", "U"));
    vertex1.put(new Tuple("key", "val", "U"));
    vertex2.put(new Tuple("key", "val", "U"));

    edge.put(new Tuple("edgeProp1", "edgeVal1", "ADMIN"));

    entityGraphStore.save(Arrays.asList(new Entity[]{vertex1, vertex2, edge}));
  }

  @Test
  public void testVertices_HasFullProperties_resultsReturned() {

    //TODO: Need to provide the "range optimization" for extracting ranges into a filter
    query = graph.query();
    CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.has("key1", "val1").has("key2", "val2").vertices();
    assertEquals(1, Iterables.size(vertices));
    assertEntitiesEqual(vertex1, ((EntityVertex)get(vertices, 0)).getEntity());

    query = graph.query();
    vertices = (CloseableIterable<Vertex>) query.has("key", "val").vertices();
    assertEquals(2, Iterables.size(vertices));
    assertEntitiesEqual(vertex1, ((EntityVertex)get(vertices, 0)).getEntity());
    assertEntitiesEqual(vertex2, ((EntityVertex)get(vertices, 1)).getEntity());
  }

  @Test
  public void testVertices_HasKeys_resultsReturned() {

    query = graph.query();
    CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.has("key1").vertices();
    assertEquals(1, Iterables.size(vertices));
    assertEntitiesEqual(vertex1, ((EntityVertex)get(vertices, 0)).getEntity());

    query = graph.query();
    vertices = (CloseableIterable<Vertex>) query.has("key").vertices();
    assertEquals(2, Iterables.size(vertices));
    assertEntitiesEqual(vertex1, ((EntityVertex)get(vertices, 0)).getEntity());
    assertEntitiesEqual(vertex2, ((EntityVertex)get(vertices, 1)).getEntity());
  }

  @Test
  public void testVertices_HasNotKeys_resultsReturned() {

    query = graph.query();
    CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.hasNot("key1").vertices();
    assertEquals(1, Iterables.size(vertices));
    assertEntitiesEqual(vertex2, ((EntityVertex)get(vertices, 0)).getEntity());

    query = graph.query();
    vertices = (CloseableIterable<Vertex>) query.hasNot("key").vertices();
    assertEquals(0, Iterables.size(vertices));

  }

  private void assertEntitiesEqual(Entity expected, Entity actual) {

    assertEquals(expected.getType(),actual.getType());
    assertEquals(expected.getId(), actual.getId());
    assertEquals(new HashSet(expected.getTuples()), new HashSet(actual.getTuples()));
  }

}
