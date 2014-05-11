package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.impl.AccumuloEntityGraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityEdge;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

public class BlueprintsGraphStoreTest {

  AccumuloEntityGraphStore entityGraphStore;
  BlueprintsGraphStore graph;
  Connector connector;

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

    edge.put(new Tuple("edgeProp1", "edgeVal1", "ADMIN"));

    entityGraphStore.save(Arrays.asList(new Entity[] { vertex1, vertex2, edge }));

  }

  @Test
  public void testGetVertex() {
    // get first vertex
    EntityVertex v = (EntityVertex)graph.getVertex(new EntityIndex(vertex1));
    assertEntitiesEqual(vertex1, v.getEntity());

    // get second vertex
    v = (EntityVertex)graph.getVertex(new EntityIndex(vertex2));
    assertEntitiesEqual(vertex2, v.getEntity());

  }

  @Test
  public void testGetEdge() {
    EntityEdge e = (EntityEdge)graph.getEdge(new EntityIndex(edge));
    assertEntitiesEqual(edge, e.getEntity());
  }

  @Test
  public void testVertices() {

    CloseableIterable<Vertex> vertices = graph.getVertices();
    assertEquals(2, Iterables.size(vertices));

    EntityVertex entity1 = (EntityVertex) Iterables.get(vertices, 0);
    assertEntitiesEqual(vertex1, entity1.getEntity());

    EntityVertex entity2 = (EntityVertex) Iterables.get(vertices, 1);
    assertEntitiesEqual(vertex2, entity2.getEntity());
  }

  @Test
  public void testEdges() {

    CloseableIterable<Edge> edges = graph.getEdges();

    assertEquals(1, Iterables.size(edges));

    EntityEdge actualEdge = (EntityEdge)Iterables.get(edges, 0);
    assertEntitiesEqual(edge, actualEdge.getEntity());
  }

  @Test
  public void testVertices_propertyQuery() {

    CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>)graph.getVertices("key1", "val1");

    assertEquals(1, Iterables.size(vertices));

    EntityVertex actualVertex = (EntityVertex)Iterables.get(vertices, 0);
    assertEntitiesEqual(vertex1, actualVertex.getEntity());

    vertices = (CloseableIterable<Vertex>)graph.getVertices("key3", "val3");

    assertEquals(1, Iterables.size(vertices));

    actualVertex = (EntityVertex)Iterables.get(vertices, 0);
    assertEntitiesEqual(vertex2, actualVertex.getEntity());
  }

  @Test
  public void testEdges_propertyQuery() {

    CloseableIterable<Edge> edges = graph.getEdges("edgeProp1", "edgeVal1");

    assertEquals(1, Iterables.size(edges));

    EntityEdge actualEdge = (EntityEdge)Iterables.get(edges, 0);
    assertEntitiesEqual(edge, actualEdge.getEntity());
  }


  private void assertEntitiesEqual(Entity expected, Entity actual) {

    assertEquals(new HashSet(expected.getTuples()), new HashSet(actual.getTuples()));
    assertEquals(expected.getType(),actual.getType());
    assertEquals(expected.getId(), actual.getId());
  }


}
