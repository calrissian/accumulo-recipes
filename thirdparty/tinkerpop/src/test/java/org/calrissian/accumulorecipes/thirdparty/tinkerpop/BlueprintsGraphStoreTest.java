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

import static java.util.Arrays.asList;
import java.util.HashSet;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.calrissian.mango.domain.entity.EntityIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlueprintsGraphStoreTest {

    AccumuloEntityGraphStore entityGraphStore;
    EntityGraph graph;
    Connector connector;

    Entity vertex1 = new BaseEntity("vertexType1", "id1");
    Entity vertex2 = new BaseEntity("vertexType2", "id2");
    Entity edge = new EdgeEntity("edgeType1", "edgeId", vertex1, "", vertex2, "", "label1");

    @Before
    public void start() throws Exception {
        Instance instance = new MockInstance();
        connector = instance.getConnector("root", new PasswordToken(""));
        entityGraphStore = new AccumuloEntityGraphStore(connector);
        graph = new EntityGraph(entityGraphStore, Sets.newHashSet("vertexType1", "vertexType2"),
                Sets.newHashSet("edgeType1", "edgeType2"),
                new Auths("U,ADMIN"));


        Attribute attribute = new Attribute("key1", "val1", new MetadataBuilder().setVisibility("U").build());
        Attribute attribute2 = new Attribute("key2", "val2", new MetadataBuilder().setVisibility("U").build());
        Attribute attribute3 = new Attribute("key3", "val3", new MetadataBuilder().setVisibility("U").build());
        Attribute attribute4 = new Attribute("key4", "val4", new MetadataBuilder().setVisibility("U").build());

        vertex1.put(attribute);
        vertex1.put(attribute2);
        vertex2.put(attribute3);
        vertex2.put(attribute4);

        Attribute edgeAttribute = new Attribute("edgeProp1", "edgeVal1", new MetadataBuilder().setVisibility("ADMIN").build());
        edge.put(edgeAttribute);

        entityGraphStore.save(asList(vertex1, vertex2, edge));

    }

    @Test
    public void testGetVertex() {
        // get first vertex
        EntityVertex v = (EntityVertex) graph.getVertex(new EntityIndex(vertex1.getType(), vertex1.getId()));
        assertEntitiesEqual(vertex1, v.getEntity());

        // get second vertex
        v = (EntityVertex) graph.getVertex(new EntityIndex(vertex2.getType(), vertex2.getId()));
        assertEntitiesEqual(vertex2, v.getEntity());

    }

    @Test
    public void testGetEdge() {
        EntityEdge e = (EntityEdge) graph.getEdge(new EntityIndex(edge.getType(), edge.getId()));
        assertEntitiesEqual(edge, e.getEntity());
    }

    @Test
    public void testVertices() {

        CloseableIterable<Vertex> vertices = graph.getVertices();
        Assert.assertEquals(2, Iterables.size(vertices));

        EntityVertex entity1 = (EntityVertex) Iterables.get(vertices, 0);
        assertEntitiesEqual(vertex1, entity1.getEntity());

        EntityVertex entity2 = (EntityVertex) Iterables.get(vertices, 1);
        assertEntitiesEqual(vertex2, entity2.getEntity());
    }

    @Test
    public void testEdges() throws Exception {

        CloseableIterable<Edge> edges = graph.getEdges();

        Assert.assertEquals(1, Iterables.size(edges));

        EntityEdge actualEdge = (EntityEdge) Iterables.get(edges, 0);
        assertEntitiesEqual(edge, actualEdge.getEntity());
    }

    @Test
    public void testVertices_propertyQuery() {

        CloseableIterable<Vertex> vertices = graph.getVertices("key1", "val1");

        Assert.assertEquals(1, Iterables.size(vertices));

        EntityVertex actualVertex = (EntityVertex) Iterables.get(vertices, 0);
        assertEntitiesEqual(vertex1, actualVertex.getEntity());

        vertices = graph.getVertices("key3", "val3");

        Assert.assertEquals(1, Iterables.size(vertices));

        actualVertex = (EntityVertex) Iterables.get(vertices, 0);
        assertEntitiesEqual(vertex2, actualVertex.getEntity());
    }

    @Test
    public void testEdges_propertyQuery() {

        CloseableIterable<Edge> edges = graph.getEdges("edgeProp1", "edgeVal1");

        Assert.assertEquals(1, Iterables.size(edges));

        EntityEdge actualEdge = (EntityEdge) Iterables.get(edges, 0);
        assertEntitiesEqual(edge, actualEdge.getEntity());
    }


    private void assertEntitiesEqual(Entity expected, Entity actual) {

        Assert.assertEquals(new HashSet(expected.getAttributes()), new HashSet(actual.getAttributes()));
        Assert.assertEquals(expected.getType(), actual.getType());
        Assert.assertEquals(expected.getId(), actual.getId());
    }


}
