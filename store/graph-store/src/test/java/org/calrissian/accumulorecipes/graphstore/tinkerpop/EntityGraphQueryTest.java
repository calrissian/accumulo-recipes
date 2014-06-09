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
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.newHashSet;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.setVisibility;
import static org.junit.Assert.assertEquals;

public class EntityGraphQueryTest {


    AccumuloEntityGraphStore entityGraphStore;
    EntityGraph graph;
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
        graph = new EntityGraph(entityGraphStore, newHashSet("vertexType1", "vertexType2"),
                newHashSet("edgeType1", "edgeType2"),
                new Auths("U,ADMIN"));

        Tuple v1t1 = new Tuple("key1", "val1", setVisibility(new HashMap<String, Object>(1), "U"));
        vertex1.put(v1t1);

        Tuple v1t2 = new Tuple("key2", "val2", setVisibility(new HashMap<String, Object>(1), "U"));
        vertex1.put(v1t2);

        Tuple v2t1 = new Tuple("key3", "val3", setVisibility(new HashMap<String, Object>(1), "U"));
        vertex2.put(v2t1);

        Tuple v2t2 = new Tuple("key4", "val4", setVisibility(new HashMap<String, Object>(1), "U"));
        vertex2.put(v2t2);

        Tuple keyVal = new Tuple("key", "val", setVisibility(new HashMap<String, Object>(1), "U"));
        vertex1.put(keyVal);
        vertex2.put(keyVal);

        Tuple e1t1 = new Tuple("edgeProp1", "edgeVal1", setVisibility(new HashMap<String, Object>(1), "ADMIN"));
        edge.put(e1t1);

        Tuple e1t2 = new Tuple("edgeProp2", "edgeVal2", setVisibility(new HashMap<String, Object>(1), "U"));
        edge.put(e1t2);

        Tuple e2t1 = new Tuple("edgeProp3", "edgeVal3", setVisibility(new HashMap<String, Object>(1), "ADMIN"));
        edge2.put(e2t1);

        Tuple e2t2 = new Tuple("edgeProp4", "edgeVal4", setVisibility(new HashMap<String, Object>(1), "U"));
        edge2.put(e2t2);

        Tuple edgeKeyVal = new Tuple("edgeProp", "edgeVal", setVisibility(new HashMap<String, Object>(1), "U"));

        edge.put(edgeKeyVal);
        edge2.put(edgeKeyVal);



        entityGraphStore.save(Arrays.asList(new Entity[]{vertex1, vertex2, edge, edge2}));
    }

    @Test
    public void testVertices_HasFullProperties_resultsReturned() {

        query = graph.query();
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.has("key1", "val1").has("key2", "val2").vertices();
        assertEquals(1, Iterables.size(vertices));
        assertEntitiesEqual(vertex1, ((EntityVertex) get(vertices, 0)).getEntity());

        query = graph.query();
        vertices = (CloseableIterable<Vertex>) query.has("key", "val").vertices();
        assertEquals(2, Iterables.size(vertices));
        assertEntitiesEqual(vertex1, ((EntityVertex) get(vertices, 0)).getEntity());
        assertEntitiesEqual(vertex2, ((EntityVertex) get(vertices, 1)).getEntity());
    }

    @Test
    public void testVertices_HasKeys_resultsReturned() {

        query = graph.query();
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.has("key1").vertices();
        assertEquals(1, Iterables.size(vertices));
        assertEntitiesEqual(vertex1, ((EntityVertex) get(vertices, 0)).getEntity());

        query = graph.query();
        vertices = (CloseableIterable<Vertex>) query.has("key").vertices();
        assertEquals(2, Iterables.size(vertices));
        assertEntitiesEqual(vertex1, ((EntityVertex) get(vertices, 0)).getEntity());
        assertEntitiesEqual(vertex2, ((EntityVertex) get(vertices, 1)).getEntity());
    }

    @Test
    public void testVertices_HasNotKeys_resultsReturned() {

        query = graph.query();
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.hasNot("key1").vertices();
        assertEquals(1, Iterables.size(vertices));
        assertEntitiesEqual(vertex2, ((EntityVertex) get(vertices, 0)).getEntity());

        query = graph.query();
        vertices = (CloseableIterable<Vertex>) query.hasNot("key").vertices();
        assertEquals(0, Iterables.size(vertices));
    }

    @Test
    public void testVertices_NoQuery_Limit() {

        query = graph.query();
        CloseableIterable<Vertex> vertices = (CloseableIterable<Vertex>) query.limit(1).vertices();
        assertEquals(1, Iterables.size(vertices));
        assertEntitiesEqual(vertex1, ((EntityVertex) get(vertices, 0)).getEntity());

        query = graph.query();
        vertices = (CloseableIterable<Vertex>) query.limit(0).vertices();
        assertEquals(0, Iterables.size(vertices));
    }

    @Test
    public void testEdges_HasFullProperties_resultsReturned() {

        query = graph.query();
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) query.has("edgeProp1", "edgeVal1").has("edgeProp2", "edgeVal2").edges();
        assertEquals(1, Iterables.size(edges));
        assertEntitiesEqual(edge, ((EntityEdge) get(edges, 0)).getEntity());

        query = graph.query();
        edges = (CloseableIterable<Edge>) query.has("edgeProp", "edgeVal").edges();
        assertEquals(2, Iterables.size(edges));
        assertEntitiesEqual(edge, ((EntityEdge) get(edges, 0)).getEntity());
        assertEntitiesEqual(edge2, ((EntityEdge) get(edges, 1)).getEntity());
    }

    @Test
    public void testEdges_HasKeys_resultsReturned() {
        query = graph.query();
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) query.has("edgeProp1").edges();
        assertEquals(1, Iterables.size(edges));
        assertEntitiesEqual(edge, ((EntityEdge) get(edges, 0)).getEntity());

        query = graph.query();
        edges = (CloseableIterable<Edge>) query.has("edgeProp").edges();
        assertEquals(2, Iterables.size(edges));
        assertEntitiesEqual(edge, ((EntityEdge) get(edges, 0)).getEntity());
        assertEntitiesEqual(edge2, ((EntityEdge) get(edges, 1)).getEntity());
    }

    @Test
    public void testEdges_HasNotKeys_resultsReturned() {
        query = graph.query();
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) query.hasNot("edgeProp1").edges();
        assertEquals(1, Iterables.size(edges));
        assertEntitiesEqual(edge2, ((EntityEdge) get(edges, 0)).getEntity());

        query = graph.query();
        edges = (CloseableIterable<Edge>) query.hasNot("edgeProp").edges();
        assertEquals(0, Iterables.size(edges));
    }



    @Test
    public void testEdges_NoQuery_Limit() {

        query = graph.query();
        CloseableIterable<Edge> edges = (CloseableIterable<Edge>) query.limit(1).edges();
        assertEquals(1, Iterables.size(edges));
        assertEntitiesEqual(edge, ((EntityEdge) get(edges, 0)).getEntity());

        query = graph.query();
        edges = (CloseableIterable<Edge>) query.limit(0).edges();
        assertEquals(0, Iterables.size(edges));
    }

    private void assertEntitiesEqual(Entity expected, Entity actual) {

        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(new HashSet(expected.getTuples()), new HashSet(actual.getTuples()));
    }

}
