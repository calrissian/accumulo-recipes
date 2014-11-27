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
package org.calrissian.accumulorecipes.entitystore.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.tuple.MetadataBuilder;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIndex;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Iterables.size;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AccumuloEntityStoreTest {

    private EntityStore store;
    private Connector connector;

    private Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }
    private Map<String, Object> meta = new MetadataBuilder().setVisibility("A").build();
    private Auths DEFAULT_AUTHS = new Auths("A");

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        connector = getConnector();
        connector.securityOperations().createLocalUser("root", new PasswordToken(""));
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("A"));
        store = new AccumuloEntityStore(connector);
    }

    @Test
    public void testGet() throws Exception {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", meta));

        store.save(asList(entity, entity2));

        Scanner scanner = connector.createScanner(DEFAULT_SHARD_TABLE_NAME, new Authorizations());
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println("ENTRY: " + entry);
        }

        CloseableIterable<Entity> actualEntity = store.get(singletonList(new EntityIndex("type", "id")), null, DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(actualEntity));
        Entity actual = actualEntity.iterator().next();
        assertEquals(new HashSet(actual.getTuples()), new HashSet(entity.getTuples()));
        assertEquals(actual.getId(), entity.getId());
        assertEquals(actual.getType(), entity.getType());
    }


    @Test
    public void testVisibility() {

        Map<String, Object> shouldntSee = new MetadataBuilder().setVisibility("A&B").build();

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", shouldntSee));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", shouldntSee));

        store.save(asList(entity, entity2));

        List<EntityIndex> indexes = asList(new EntityIndex[] {
            new EntityIndex(entity.getType(), entity.getId()),
            new EntityIndex(entity2.getType(),entity2.getId())
        });

        Iterable<Entity> actualEntities = store.get(indexes, null, new Auths("A"));

        assertEquals(2, Iterables.size(actualEntities));
        assertEquals(1, Iterables.get(actualEntities, 0).size());
        assertEquals(1, Iterables.get(actualEntities, 1).size());
    }


    @Test
    public void testGreaterThan() throws Exception {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", 1, meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", 9, meta));

        store.save(asList(entity, entity2));

        Scanner scanner = connector.createScanner(DEFAULT_SHARD_TABLE_NAME, new Authorizations());
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println("ENTRY: " + entry);
        }

        CloseableIterable<Entity> actualEntity = store.query(singleton("type"),
            new QueryBuilder().greaterThan("key2", 8).build(), null, DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(actualEntity));
        Entity actual = actualEntity.iterator().next();
        assertEquals(new HashSet(actual.getTuples()), new HashSet(entity2.getTuples()));
        assertEquals(actual.getId(), entity2.getId());
        assertEquals(actual.getType(), entity2.getType());
    }

    @Test
    public void testQueryKeyNotInIndex() {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", meta));

        store.save(asList(entity, entity2));

        Node query = new QueryBuilder().and().eq("key5", "val5").end().build();

        Iterable<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS);

        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQueryRangeNotInIndex() {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", meta));

        store.save(asList(entity, entity2));

        Node query = new QueryBuilder().and().range("key5", 0, 5).end().build();

        Iterable<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS);

        assertEquals(0, Iterables.size(itr));
    }



    @Test
    public void testGet_withSelection() throws Exception {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", meta));

        store.save(asList(entity, entity2));

        CloseableIterable<Entity> actualEvent = store.get(singletonList(new EntityIndex("type", entity.getId())),
                singleton("key1"), DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(actualEvent));
        assertNull(actualEvent.iterator().next().get("key2"));
        assertNotNull(actualEvent.iterator().next().get("key1"));
    }


    @Test
    public void testQuery_withSelection() throws Exception {
        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", meta));

        store.save(asList(entity, entity2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterable<Entity> itr = store.query(singleton("type"), query, singleton("key1"), DEFAULT_AUTHS);

        int count = 0;
        for (Entity entry : itr) {
            count++;
            assertNull(entry.get("key2"));
            assertNotNull(entry.get("key1"));
        }
        assertEquals(2, count);
    }

    @Test
    public void testQuery_AndQuery() throws Exception {
        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key2", "val2", meta));

        store.save(asList(entity, entity2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        Entity actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity.getTuples()));
        else
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity2.getTuples()));

        actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity.getTuples()));
        else
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity2.getTuples()));
    }

    @Test
    public void testQuery_OrQuery() throws Exception {
        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id3");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key3", "val3", meta));

        store.save(asList(entity, entity2));

        AccumuloTestUtils.dumpTable(connector, "entity_shard");

        Node query = new QueryBuilder().or().eq("key3", "val3").eq("key2", "val2").end().build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        Entity actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
        else
            assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));

        actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
        else
            assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {
        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));
        entity.put(new Tuple("key2", "val2", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", "val1", meta));
        entity2.put(new Tuple("key3", "val3", meta));

        store.save(asList(entity, entity2));

        Node query = new QueryBuilder().eq("key1", "val1").build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        Entity actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));

        else
            assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));

        actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
        else
            assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));
    }

    @Test
    public void testQuery_has() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));

        store.save(asList(entity));

        Node node = new QueryBuilder().has("key1").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
        assertEquals(entity, Iterables.get(itr, 0));
    }

    @Test
    public void testQuery_hasNot() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));

        store.save(asList(entity));

        Node node = new QueryBuilder().has("key2").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQuery_in_noResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));

        store.save(asList(entity));

        Node node = new QueryBuilder().in("key1", "val2", "val3", "val4").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }

    @Test
    public void testQuery_in_results() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));

        store.save(asList(entity));

        Node node = new QueryBuilder().and().in("key1", "val1", "val2", "val3").eq("key1", "val1").end().build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
    }

    @Test
    public void testQuery_notIn_noResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", "val1", meta));

        store.save(asList(entity));

        Node node = new QueryBuilder().notIn("key1", "val1", "val2", "val3").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }




    @Test
    public void testQuery_greaterThan() {

        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", 5, meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", 10, meta));

        store.save(asList(entity, entity2));

        Node node = new QueryBuilder().greaterThan("key1", 4).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null,DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = new QueryBuilder().greaterThan("key1", 9).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));

        node = new QueryBuilder().greaterThan("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }

    @Test
    public void testQuery_greaterThanEq() {


        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", 5, meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", 10, meta));

        store.save(asList(entity, entity2));

        Node node = new QueryBuilder().greaterThanEq("key1", 4).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = new QueryBuilder().greaterThanEq("key1", 9).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));

        node = new QueryBuilder().greaterThanEq("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
    }

    @Test
    public void testQuery_lessThan() {


        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", 5, meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", 10, meta));

        store.save(asList(entity, entity2));

        Node node = new QueryBuilder().lessThan("key1", 11).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = new QueryBuilder().lessThan("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));

        node = new QueryBuilder().lessThan("key1", 5).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQuery_lessThanEq() throws TableNotFoundException {


        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("key1", 5, meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("key1", 10, meta));

        store.save(asList(entity, entity2));

        Node node = new QueryBuilder().lessThanEq("key1", 11).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = new QueryBuilder().lessThanEq("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        AccumuloTestUtils.dumpTable(connector, "entity_shard", new Authorizations());

        node = new QueryBuilder().lessThanEq("key1", 5).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
    }

    @Test
    public void testKeys() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("hasIp", "true", meta));
        entity.put(new Tuple("ip", "1.1.1.1", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("hasIp", "true", meta));
        entity2.put(new Tuple("ip", "2.2.2.2", meta));

        Entity entity3 = new BaseEntity("type", "id3");
        entity3.put(new Tuple("hasIp", "true", meta));
        entity3.put(new Tuple("ip", "3.3.3.3", meta));

        store.save(asList(entity, entity2, entity3));

        CloseableIterable<Pair<String, String>> keys = store.keys("type", DEFAULT_AUTHS);

        assertEquals(2, Iterables.size(keys));
        assertEquals(new Pair<String, String>("hasIp", "string"), Iterables.get(keys, 0));
        assertEquals(new Pair<String, String>("ip", "string"), Iterables.get(keys, 1));
    }

    @Test
    public void testQuery_MultipleNotInQuery() throws Exception {
        Entity entity = new BaseEntity("type", "id");
        entity.put(new Tuple("hasIp", "true", meta));
        entity.put(new Tuple("ip", "1.1.1.1", meta));

        Entity entity2 = new BaseEntity("type", "id2");
        entity2.put(new Tuple("hasIp", "true", meta));
        entity2.put(new Tuple("ip", "2.2.2.2", meta));

        Entity entity3 = new BaseEntity("type", "id3");
        entity3.put(new Tuple("hasIp", "true", meta));
        entity3.put(new Tuple("ip", "3.3.3.3", meta));

        store.save(asList(entity, entity2, entity3));

        Node query = new QueryBuilder()
                .and()
                .notEq("ip", "1.1.1.1")
                .notEq("ip", "2.2.2.2")
                .notEq("ip", "4.4.4.4")
                .eq("hasIp", "true")
                .end().build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        int x = 0;

        while (itr.hasNext()) {
            x++;
            Entity e = itr.next();
            assertEquals("3.3.3.3", e.get("ip").getValue());

        }
        assertEquals(1, x);
    }


    @Test
    public void testQuery_emptyNodeReturnsNoResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Node query = new QueryBuilder().and().end().build();

        CloseableIterable<Entity> itr = store.query(Collections.singleton("type"), query, null, DEFAULT_AUTHS);

        assertEquals(0, size(itr));
    }

}
