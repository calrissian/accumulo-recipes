package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.GraphQuery;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
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

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

public class EntityVertexQueryTest {

  AccumuloEntityGraphStore entityGraphStore;
  BlueprintsGraphStore graph;
  Connector connector;
  GraphQuery query;

  Entity vertex1 = new BaseEntity("vertexType1", "id1");
  Entity vertex2 = new BaseEntity("vertexType2", "id2");
  Entity edge = new EdgeEntity("edgeType1", "edgeId", vertex1, "", vertex2, "", "label1");
  Entity edge2 = new EdgeEntity("edgeType2", "edgeId2", vertex1, "", vertex2, "", "label2");

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
    edge.put(new Tuple("edgeProp2", "edgeVal2", "U"));
    edge2.put(new Tuple("edgeProp3", "edgeVal3", "ADMIN"));
    edge2.put(new Tuple("edgeProp4", "edgeVal4", "U"));
    edge.put(new Tuple("edgeProp", "edgeVal", "U"));
    edge2.put(new Tuple("edgeProp", "edgeVal", "U"));

    entityGraphStore.save(Arrays.asList(new Entity[]{vertex1, vertex2, edge, edge2}));
  }


  @Test
  public void testCount_noQuery() {
    EntityVertex v1 = (EntityVertex) graph.getVertex(new EntityIndex(vertex1));
    assertEquals(2, v1.query().count());
  }

  @Test
  public void testCount_query() {
    EntityVertex v1 = (EntityVertex) graph.getVertex(new EntityIndex(vertex1));
    assertEquals(1, v1.query().has("edgeProp1").count());
  }

  @Test
  public void testVertexIds_noQuery_noLabels() {
    EntityVertex v1 = (EntityVertex) graph.getVertex(new EntityIndex(vertex1));
    CloseableIterable<EntityIndex> vertexIds = (CloseableIterable<EntityIndex>) v1.query().vertexIds();
    System.out.println(vertexIds);
    assertEquals(2, Iterables.size(vertexIds));
    // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
    assertEquals(new EntityIndex(vertex2), Iterables.get(vertexIds, 0));
    assertEquals(new EntityIndex(vertex2), Iterables.get(vertexIds, 1));
  }

  @Test
  public void testVertexIds_withLabels() {
    EntityVertex v1 = (EntityVertex) graph.getVertex(new EntityIndex(vertex1));
    CloseableIterable<EntityIndex> vertexIds = (CloseableIterable<EntityIndex>) v1.query().labels("label1").vertexIds();
    assertEquals(1, Iterables.size(vertexIds));
    // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
    assertEquals(new EntityIndex(vertex2), Iterables.get(vertexIds, 0));
  }

  @Test
  public void testVertexIds_query_withLabels() {
    EntityVertex v1 = (EntityVertex) graph.getVertex(new EntityIndex(vertex1));
    CloseableIterable<EntityIndex> vertexIds = (CloseableIterable<EntityIndex>) v1.query().has("edgeProp1").labels("label1").vertexIds();
    assertEquals(1, Iterables.size(vertexIds));
    // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
    assertEquals(new EntityIndex(vertex2), Iterables.get(vertexIds, 0));
  }

  private void assertEntitiesEqual(Entity expected, Entity actual) {

    assertEquals(new HashSet(expected.getTuples()), new HashSet(actual.getTuples()));
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getId(), actual.getId());
  }

}
