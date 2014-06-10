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
package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.tuple.MetadataBuilder;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.impl.AccumuloEntityGraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityEdge;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class BlueprintsGraphStoreTest {

    AccumuloEntityGraphStore entityGraphStore;
    EntityGraph graph;
    Connector connector;

    Entity vertex1 = new BaseEntity("vertexType1", "id1");
    Entity vertex2 = new BaseEntity("vertexType2", "id2");
    Entity edge = new EdgeEntity("edgeType1", "edgeId", vertex1, "", vertex2, "", "label1");

    @Before
    public void start() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "".getBytes());
        entityGraphStore = new AccumuloEntityGraphStore(connector);
        graph = new EntityGraph(entityGraphStore, newHashSet("vertexType1", "vertexType2"),
                newHashSet("edgeType1", "edgeType2"),
                new Auths("U,ADMIN"));


        Tuple tuple = new Tuple("key1", "val1", new MetadataBuilder().setVisibility("U").build());
        Tuple tuple2 = new Tuple("key2", "val2", new MetadataBuilder().setVisibility("U").build());
        Tuple tuple3 = new Tuple("key3", "val3", new MetadataBuilder().setVisibility("U").build());
        Tuple tuple4 = new Tuple("key4", "val4", new MetadataBuilder().setVisibility("U").build());

        vertex1.put(tuple);
        vertex1.put(tuple2);
        vertex2.put(tuple3);
        vertex2.put(tuple4);

        Tuple edgeTuple = new Tuple("edgeProp1", "edgeVal1", new MetadataBuilder().setVisibility("ADMIN").build());
        edge.put(edgeTuple);

        entityGraphStore.save(asList(vertex1, vertex2, edge));

    }

    @Test
    public void testGetVertex() {
        // get first vertex
        EntityVertex v = (EntityVertex) graph.getVertex(new EntityIndex(vertex1));
        assertEntitiesEqual(vertex1, v.getEntity());

        // get second vertex
        v = (EntityVertex) graph.getVertex(new EntityIndex(vertex2));
        assertEntitiesEqual(vertex2, v.getEntity());

    }

    @Test
    public void testGetEdge() {
        EntityEdge e = (EntityEdge) graph.getEdge(new EntityIndex(edge));
        assertEntitiesEqual(edge, e.getEntity());
    }

    @Test
    public void testVertices() {

        CloseableIterable<Vertex> vertices = graph.getVertices();
        assertEquals(2, size(vertices));

        EntityVertex entity1 = (EntityVertex) Iterables.get(vertices, 0);
        assertEntitiesEqual(vertex1, entity1.getEntity());

        EntityVertex entity2 = (EntityVertex) Iterables.get(vertices, 1);
        assertEntitiesEqual(vertex2, entity2.getEntity());
    }

    @Test
    public void testEdges() {

        CloseableIterable<Edge> edges = graph.getEdges();

        assertEquals(1, size(edges));

        EntityEdge actualEdge = (EntityEdge) Iterables.get(edges, 0);
        assertEntitiesEqual(edge, actualEdge.getEntity());
    }

    @Test
    public void testVertices_propertyQuery() {

        CloseableIterable<Vertex> vertices = graph.getVertices("key1", "val1");

        assertEquals(1, size(vertices));

        EntityVertex actualVertex = (EntityVertex) Iterables.get(vertices, 0);
        assertEntitiesEqual(vertex1, actualVertex.getEntity());

        vertices = graph.getVertices("key3", "val3");

        assertEquals(1, size(vertices));

        actualVertex = (EntityVertex) Iterables.get(vertices, 0);
        assertEntitiesEqual(vertex2, actualVertex.getEntity());
    }

    @Test
    public void testEdges_propertyQuery() {

        CloseableIterable<Edge> edges = graph.getEdges("edgeProp1", "edgeVal1");

        assertEquals(1, size(edges));

        EntityEdge actualEdge = (EntityEdge) Iterables.get(edges, 0);
        assertEntitiesEqual(edge, actualEdge.getEntity());
    }


    private void assertEntitiesEqual(Entity expected, Entity actual) {

        assertEquals(new HashSet(expected.getTuples()), new HashSet(actual.getTuples()));
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getId(), actual.getId());
    }


}
