/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.thirdparty.tinkerpop;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.attribute.MetadataBuilder;
import org.calrissian.accumulorecipes.graphstore.impl.AccumuloEntityGraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.thirdparty.tinkerpop.model.EntityEdge;
import org.calrissian.accumulorecipes.thirdparty.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIdentifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EntityVertexQueryTest {

    AccumuloEntityGraphStore entityGraphStore;
    EntityGraph graph;
    Connector connector;

    Entity vertex1 = new BaseEntity("vertexType1", "id1");
    Entity vertex2 = new BaseEntity("vertexType2", "id2");
    Entity edge = new EdgeEntity("edgeType1", "edgeId", vertex1, "", vertex2, "", "label1");
    Entity edge2 = new EdgeEntity("edgeType2", "edgeId2", vertex1, "", vertex2, "", "label2");
    Entity vertex3 = new BaseEntity("vertexType2", "id3");
    Entity edge3 = new EdgeEntity("edgeType2", "edgeId3", vertex3, "", vertex1, "", "label1");

    private static EntityIdentifier entityIndex(Entity entity) {
        return new EntityIdentifier(entity.getType(), entity.getId());
    }

    @Before
    public void start() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "".getBytes());
        entityGraphStore = new AccumuloEntityGraphStore(connector);
        graph = new EntityGraph(entityGraphStore, Sets.newHashSet("vertexType1", "vertexType2"),
                Sets.newHashSet("edgeType1", "edgeType2"),
                new Auths("U,ADMIN"));


        Attribute v1t1 = new Attribute("key1", "val1", new MetadataBuilder().setVisibility("U").build());
        vertex1.put(v1t1);

        Attribute v1t2 = new Attribute("key2", "val2", new MetadataBuilder().setVisibility("U").build());
        vertex1.put(v1t2);

        Attribute v2t1 = new Attribute("key3", "val3", new MetadataBuilder().setVisibility("U").build());
        vertex2.put(v2t1);

        Attribute v2t2 = new Attribute("key4", "val4", new MetadataBuilder().setVisibility("U").build());
        vertex2.put(v2t2);

        Attribute v3t1 = new Attribute("key5", "val5", new MetadataBuilder().setVisibility("U").build());
        vertex3.put(v3t1);

        Attribute keyVal = new Attribute("key", "val", new MetadataBuilder().setVisibility("U").build());
        vertex1.put(keyVal);
        vertex2.put(keyVal);

        Attribute e1t1 = new Attribute("edgeProp1", "edgeVal1", new MetadataBuilder().setVisibility("ADMIN").build());
        edge.put(e1t1);

        Attribute e1t2 = new Attribute("edgeProp2", "edgeVal2", new MetadataBuilder().setVisibility("U").build());
        edge.put(e1t2);

        Attribute e2t1 = new Attribute("edgeProp3", "edgeVal3", new MetadataBuilder().setVisibility("ADMIN").build());
        edge2.put(e2t1);

        Attribute e2t2 = new Attribute("edgeProp4", "edgeVal4", new MetadataBuilder().setVisibility("U").build());
        edge2.put(e2t2);

        Attribute edgeKeyVal = new Attribute("edgeProp", "edgeVal", new MetadataBuilder().setVisibility("U").build());

        edge.put(edgeKeyVal);
        edge2.put(edgeKeyVal);

        Attribute e3t1 = new Attribute("edgeProp3", "edgeVal3", new MetadataBuilder().setVisibility("ADMIN").build());
        edge3.put(e3t1);

        entityGraphStore.save(Arrays.asList(vertex1, vertex2, vertex3, edge, edge2, edge3));
    }


    @Test
    public void testCount_noQuery_defaultDirection() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        assertEquals(3, v1.query().count());
    }


    @Test
    public void testCount_noQuery_inDirection() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        assertEquals(1, v1.query().direction(Direction.IN).count());
    }


    @Test
    public void testCount_noQuery_outDirection() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        assertEquals(2, v1.query().direction(Direction.OUT).count());
    }

    @Test
    public void testCount_query_defaultDirection() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        assertEquals(1, v1.query().has("edgeProp1").count());
    }

    @Test
    public void testCount_query_inDirection() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex2));
        assertEquals(1, v1.query().direction(Direction.IN).has("edgeProp1").count());
    }

    @Test
    public void testCount_query_outDirection() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        assertEquals(1, v1.query().direction(Direction.OUT).has("edgeProp1").count());
    }

    @Test
    public void testVertexIds_noQuery_noLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<EntityIdentifier> vertexIds = (CloseableIterable<EntityIdentifier>) v1.query().vertexIds();
        System.out.println(vertexIds);
        Assert.assertEquals(3, Iterables.size(vertexIds));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        Assert.assertEquals(entityIndex(vertex3), Iterables.get(vertexIds, 0));
        Assert.assertEquals(entityIndex(vertex2), Iterables.get(vertexIds, 1));
        Assert.assertEquals(entityIndex(vertex2), Iterables.get(vertexIds, 2));
    }

    @Test
    public void testVertexIds_withLabels_noDirection() throws TableNotFoundException {
        Scanner scanner = connector.createScanner("entityStore_graph", new Authorizations("ADMIN,U".getBytes()));
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
        }
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<EntityIdentifier> vertexIds = (CloseableIterable<EntityIdentifier>) v1.query().labels("label1").vertexIds();
        Assert.assertEquals(2, Iterables.size(vertexIds));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        Assert.assertEquals(entityIndex(vertex3), Iterables.get(vertexIds, 0));
        Assert.assertEquals(entityIndex(vertex2), Iterables.get(vertexIds, 1));
    }

    @Test
    public void testVertexIds_query_withLabels() throws TableNotFoundException {

        Scanner scanner = connector.createScanner("entityStore_graph", new Authorizations("ADMIN,U".getBytes()));
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
        }
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<EntityIdentifier> vertexIds =
                (CloseableIterable<EntityIdentifier>) v1.query().has("edgeProp1").labels("label1").vertexIds();
        Assert.assertEquals(1, Iterables.size(vertexIds));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        Assert.assertEquals(entityIndex(vertex2), Iterables.get(vertexIds, 0));
    }

    @Test
    public void testVertices_noQuery_noLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) v1.query().vertices();
        System.out.println(vertices);
        Assert.assertEquals(3, Iterables.size(vertices));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        assertEntitiesEqual(vertex3, ((EntityVertex) Iterables.get(vertices, 0)).getEntity());
        assertEntitiesEqual(vertex2, ((EntityVertex) Iterables.get(vertices, 1)).getEntity());
        assertEntitiesEqual(vertex2, ((EntityVertex) Iterables.get(vertices, 2)).getEntity());
    }


    @Test
    public void testVertices_noQuery_withLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) v1.query().labels("label1").vertices();
        System.out.println(vertices);
        Assert.assertEquals(2, Iterables.size(vertices));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        assertEntitiesEqual(vertex3, ((EntityVertex) Iterables.get(vertices, 0)).getEntity());
        assertEntitiesEqual(vertex2, ((EntityVertex) Iterables.get(vertices, 1)).getEntity());
    }

    @Test
    public void testVertices_query_withLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) v1.query().has("edgeProp1", "edgeVal1").labels("label1").vertices();
        System.out.println(vertices);
        Assert.assertEquals(1, Iterables.size(vertices));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        assertEntitiesEqual(vertex2, ((EntityVertex) Iterables.get(vertices, 0)).getEntity());
    }

    @Test
    public void testEdges_noQuery_noLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) v1.query().edges();
        System.out.println(edges);
        Assert.assertEquals(3, Iterables.size(edges));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        assertEntitiesEqual(edge3, ((EntityEdge) Iterables.get(edges, 0)).getEntity());
        assertEntitiesEqual(edge, ((EntityEdge) Iterables.get(edges, 1)).getEntity());
        assertEntitiesEqual(edge2, ((EntityEdge) Iterables.get(edges, 2)).getEntity());
    }


    @Test
    public void testEdges_noQuery_withLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) v1.query().labels("label1").edges();
        System.out.println(edges);
        Assert.assertEquals(2, Iterables.size(edges));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        assertEntitiesEqual(edge3, ((EntityEdge) Iterables.get(edges, 0)).getEntity());
        assertEntitiesEqual(edge, ((EntityEdge) Iterables.get(edges, 1)).getEntity());
    }

    @Test
    public void testEdges_query_withLabels() {
        EntityVertex v1 = (EntityVertex) graph.getVertex(entityIndex(vertex1));
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) v1.query().has("edgeProp1", "edgeVal1").labels("label1").edges();
        System.out.println(edges);
        Assert.assertEquals(1, Iterables.size(edges));
        // two edges point out from vertex1 to vertex2. This should mean vertex2 shows up twice
        assertEntitiesEqual(edge, ((EntityEdge) Iterables.get(edges, 0)).getEntity());
    }


    private void assertEntitiesEqual(Entity expected, Entity actual) {

        Assert.assertEquals(new HashSet(expected.getAttributes()), new HashSet(actual.getAttributes()));
        Assert.assertEquals(expected.getType(), actual.getType());
        Assert.assertEquals(expected.getId(), actual.getId());
    }

}
