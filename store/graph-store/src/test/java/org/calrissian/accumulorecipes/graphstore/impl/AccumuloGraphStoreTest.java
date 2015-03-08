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
package org.calrissian.accumulorecipes.graphstore.impl;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

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
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIdentifier;
import org.junit.Before;
import org.junit.Test;

public class AccumuloGraphStoreTest {

    Entity vertex1 = new BaseEntity("vertex", "id1");
    Entity vertex2 = new BaseEntity("vertex", "id2");
    Entity edge = new EdgeEntity("edge", "edgeId", vertex1, "", vertex2, "", "label1");

    private AccumuloEntityGraphStore graphStore;
    private Connector connector;

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {


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
        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "".getBytes());
        graphStore = new AccumuloEntityGraphStore(connector);

        graphStore.save(asList(vertex1, edge, vertex2));
    }

    @Test
    public void testSave() throws TableNotFoundException {

        Scanner scanner = connector.createScanner("entityStore_graph", new Authorizations("ADMIN,U".getBytes()));
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
        }
    }

    @Test
    public void testAdjacentEdges_withLabels_outDirection() throws TableNotFoundException {

        Scanner scanner = connector.createScanner("entityStore_graph", new Authorizations("ADMIN,U".getBytes()));
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
        }

        CloseableIterable<EdgeEntity> results = graphStore.adjacentEdges(
                asList(new EntityIdentifier(vertex1.getType(), vertex1.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.OUT,
                singleton("label1"),
                new Auths("ADMIN,U")
        );

        assertEquals(1, size(results));

        Entity actualEdge = get(results, 0);
        assertEquals(new HashSet(edge.getAttributes()), new HashSet(actualEdge.getAttributes()));
        assertEquals(edge.getType(), actualEdge.getType());
        assertEquals(edge.getId(), actualEdge.getId());

        results.closeQuietly();
    }


    @Test
    public void testAdjacentEdges_withLabels_inDirection() throws TableNotFoundException {

        CloseableIterable<EdgeEntity> results = graphStore.adjacentEdges(
                asList(new EntityIdentifier(vertex2.getType(), vertex2.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.IN,
                singleton("label1"),
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualEdge = get(results, 0);
        assertEquals(new HashSet(edge.getAttributes()), new HashSet(actualEdge.getAttributes()));
        assertEquals(edge.getType(), actualEdge.getType());
        assertEquals(edge.getId(), actualEdge.getId());

        results.closeQuietly();
    }


    @Test
    public void testAdjacentEdges_noLabels_outDirection() throws TableNotFoundException {

        CloseableIterable<EdgeEntity> results = graphStore.adjacentEdges(
                asList(new EntityIdentifier(vertex1.getType(), vertex1.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.OUT,
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualEdge = get(results, 0);
        assertEquals(new HashSet(edge.getAttributes()), new HashSet(actualEdge.getAttributes()));
        assertEquals(edge.getType(), actualEdge.getType());
        assertEquals(edge.getId(), actualEdge.getId());

        results.closeQuietly();
    }


    @Test
    public void testAdjacentEdges_noLabels_inDirection() throws TableNotFoundException {

        CloseableIterable<EdgeEntity> results = graphStore.adjacentEdges(
                asList(new EntityIdentifier(vertex2.getType(), vertex2.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.IN,
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualEdge = get(results, 0);
        assertEquals(new HashSet(edge.getAttributes()), new HashSet(actualEdge.getAttributes()));
        assertEquals(edge.getType(), actualEdge.getType());
        assertEquals(edge.getId(), actualEdge.getId());

        results.closeQuietly();
    }

    @Test
    public void testAdjacencies_withLabels_outDirection() throws TableNotFoundException {

        CloseableIterable<Entity> results = graphStore.adjacencies(
                asList(new EntityIdentifier(vertex1.getType(), vertex1.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.OUT,
                Collections.singleton("label1"),
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualVertex2 = get(results, 0);
        assertEquals(new HashSet(vertex2.getAttributes()), new HashSet(actualVertex2.getAttributes()));
        assertEquals(vertex2.getType(), actualVertex2.getType());
        assertEquals(vertex2.getId(), actualVertex2.getId());

        results.closeQuietly();
    }

    @Test
    public void testAdjacencies_withLabels_inDirection() throws TableNotFoundException {

        CloseableIterable<Entity> results = graphStore.adjacencies(
                asList(new EntityIdentifier(vertex2.getType(), vertex2.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.IN,
                Collections.singleton("label1"),
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualVertex1 = get(results, 0);
        assertEquals(new HashSet(vertex1.getAttributes()), new HashSet(actualVertex1.getAttributes()));
        assertEquals(vertex1.getType(), actualVertex1.getType());
        assertEquals(vertex1.getId(), actualVertex1.getId());

        results.closeQuietly();
    }


    @Test
    public void testAdjacencies_noLabels_outDirection() throws TableNotFoundException {

        CloseableIterable<Entity> results = graphStore.adjacencies(
                asList(new EntityIdentifier(vertex1.getType(), vertex1.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.OUT,
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualVertex2 = get(results, 0);
        assertEquals(new HashSet(vertex2.getAttributes()), new HashSet(actualVertex2.getAttributes()));
        assertEquals(vertex2.getType(), actualVertex2.getType());
        assertEquals(vertex2.getId(), actualVertex2.getId());

        results.closeQuietly();
    }

    @Test
    public void testAdjacencies_noLabels_inDirection() throws TableNotFoundException {

        CloseableIterable<Entity> results = graphStore.adjacencies(
                asList(new EntityIdentifier(vertex2.getType(), vertex2.getId())),
                QueryBuilder.create().eq("edgeProp1", "edgeVal1").build(),
                Direction.IN,
                new Auths("U,ADMIN")
        );

        assertEquals(1, size(results));

        Entity actualVertex1 = get(results, 0);
        assertEquals(new HashSet(vertex1.getAttributes()), new HashSet(actualVertex1.getAttributes()));
        assertEquals(vertex1.getType(), actualVertex1.getType());
        assertEquals(vertex1.getId(), actualVertex1.getId());

        results.closeQuietly();
    }
}
