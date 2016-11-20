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

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.attribute.MetadataBuilder;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.calrissian.mango.domain.entity.EntityIdentifier;
import org.junit.Before;
import org.junit.Test;

public class AccumuloEntityStoreTest {

    private EntityStore store;
    private Connector connector;

    private Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }
    private Map<String, String> meta = new MetadataBuilder().setVisibility("A").build();
    private Map<String, String> meta_b = new MetadataBuilder().setVisibility("B").build();
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

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        store.save(asList(entity, entity2));

        Scanner scanner = connector.createScanner(DEFAULT_SHARD_TABLE_NAME, new Authorizations());
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println("ENTRY: " + entry);
        }

        CloseableIterable<Entity> actualEntity = store.get(singletonList(new EntityIdentifier("type", "id")), null, DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(actualEntity));
        Entity actual = actualEntity.iterator().next();
        assertEquals(new HashSet(actual.getAttributes()), new HashSet(entity.getAttributes()));
        assertEquals(actual.getId(), entity.getId());
        assertEquals(actual.getType(), entity.getType());
    }




    @Test
    public void testGet_withVisibilities() {

        Map<String, String> shouldntSee = new MetadataBuilder().setVisibility("A&B").build();

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", shouldntSee))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", shouldntSee))
            .build();

        store.save(asList(entity, entity2));

        List<EntityIdentifier> indexes = asList(new EntityIdentifier[] {
            new EntityIdentifier(entity.getType(), entity.getId()),
            new EntityIdentifier(entity2.getType(),entity2.getId())
        });

        Iterable<Entity> actualEntities = store.get(indexes, null, new Auths("A"));

        assertEquals(2, Iterables.size(actualEntities));
        assertEquals(1, get(actualEntities, 0).size());
        assertEquals(1, get(actualEntities, 1).size());
    }

    @Test
    public void testVisibilityForQuery_noResults() {

      Map<String, String> shouldntSee = new MetadataBuilder().setVisibility("A&B").build();

      Entity entity = EntityBuilder.create("type", "id1")
          .attr(new Attribute("key1", "val1", meta))
          .attr(new Attribute("key2", "val2", shouldntSee))
          .build();

      Entity entity2 = EntityBuilder.create("type", "id2")
          .attr(new Attribute("key1", "val1", meta))
          .attr(new Attribute("key2", "val2", shouldntSee))
          .build();

      store.save(asList(entity, entity2));

      Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

      Iterable<Entity> actualEntity1 = store.query(Collections.singleton("type"), query, null, new Auths("A"));

      assertEquals(0, Iterables.size(actualEntity1));

    }


    @Test
    public void testVisibilityForQuery_resultsReturned() {

      Map<String, String> canSee = new MetadataBuilder().setVisibility("A&B").build();
      Map<String, String> cantSee = new MetadataBuilder().setVisibility("A&B&C").build();

      Entity entity = EntityBuilder.create("type", "id1")
          .attr(new Attribute("key1", "val1", meta))
          .attr(new Attribute("key2", "val2", canSee))
          .attr(new Attribute("key3", "val3", cantSee))
          .build();

      Entity entity2 = EntityBuilder.create("type", "id2")
          .attr(new Attribute("key1", "val1", meta))
          .attr(new Attribute("key2", "val2", canSee))
          .attr(new Attribute("key3", "val3", cantSee))
          .build();

      store.save(asList(entity, entity2));

      Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

      Iterable<Entity> actualEntity1 = store.query(Collections.singleton("type"), query, null, new Auths("A,B"));

      assertEquals(2, Iterables.size(actualEntity1));

      assertEquals(2, Iterables.get(actualEntity1, 0).size());
      assertEquals(2, Iterables.get(actualEntity1, 1).size());
    }


    @Test
    public void testGreaterThan() throws Exception {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", 1, meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", 9, meta))
            .build();

        store.save(asList(entity, entity2));

        Scanner scanner = connector.createScanner(DEFAULT_SHARD_TABLE_NAME, new Authorizations());
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println("ENTRY: " + entry);
        }

        CloseableIterable<Entity> actualEntity = store.query(singleton("type"),
            QueryBuilder.create().greaterThan("key2", 8).build(), null, DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(actualEntity));
        Entity actual = actualEntity.iterator().next();
        assertEquals(new HashSet(actual.getAttributes()), new HashSet(entity2.getAttributes()));
        assertEquals(actual.getId(), entity2.getId());
        assertEquals(actual.getType(), entity2.getType());
    }

    @Test
    public void testQueryKeyNotInIndex() {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        store.save(asList(entity, entity2));

        Node query = QueryBuilder.create().and().eq("key5", "val5").end().build();

        Iterable<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS);

        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQueryRangeNotInIndex() {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        store.save(asList(entity, entity2));

        Node query = QueryBuilder.create().and().range("key5", 0, 5).end().build();

        Iterable<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS);

        assertEquals(0, Iterables.size(itr));
    }



    @Test
    public void testGet_withSelection() throws Exception {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        store.save(asList(entity, entity2));

        CloseableIterable<Entity> actualEvent = store.get(singletonList(new EntityIdentifier("type", entity.getId())),
                singleton("key1"), DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(actualEvent));
        assertNull(actualEvent.iterator().next().get("key2"));
        assertNotNull(actualEvent.iterator().next().get("key1"));
    }


    @Test
    public void testQuery_withSelection() throws Exception {
        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        store.save(asList(entity, entity2));

        Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

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
        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        store.save(asList(entity, entity2));

        Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        Entity actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(entity.getAttributes()));
        else
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(entity2.getAttributes()));

        actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(entity.getAttributes()));
        else
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(entity2.getAttributes()));
    }

    @Test
    public void testQuery_OrQuery() throws Exception {
        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id3")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key3", "val3", meta))
            .build();

        store.save(asList(entity, entity2));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "entity_shard", DEFAULT_AUTHS.getAuths());

        Node query = QueryBuilder.create().or().eq("key3", "val3").eq("key2", "val2").end().build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        Entity actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getAttributes()), new HashSet(actualEvent.getAttributes()));
        else
            assertEquals(new HashSet(entity2.getAttributes()), new HashSet(actualEvent.getAttributes()));

        actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getAttributes()), new HashSet(actualEvent.getAttributes()));
        else
            assertEquals(new HashSet(entity2.getAttributes()), new HashSet(actualEvent.getAttributes()));
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {
        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key2", "val2", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", "val1", meta))
            .attr(new Attribute("key3", "val3", meta))
            .build();

        store.save(asList(entity, entity2));

        Node query = QueryBuilder.create().eq("key1", "val1").build();

        Iterator<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS).iterator();

        Entity actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getAttributes()), new HashSet(actualEvent.getAttributes()));

        else
            assertEquals(new HashSet(entity2.getAttributes()), new HashSet(actualEvent.getAttributes()));

        actualEvent = itr.next();
        if (actualEvent.getId().equals(entity.getId()))
            assertEquals(new HashSet(entity.getAttributes()), new HashSet(actualEvent.getAttributes()));
        else
            assertEquals(new HashSet(entity2.getAttributes()), new HashSet(actualEvent.getAttributes()));
    }

    @Test
    public void testQuery_has() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .build();

        store.save(asList(entity));

        Node node = QueryBuilder.create().has("key1").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
        assertEquals(entity, get(itr, 0));
    }

    @Test
    public void testQuery_hasNot() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .build();

        store.save(asList(entity));

        Node node = QueryBuilder.create().has("key2").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQuery_in_noResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .build();

        store.save(asList(entity));

        Node node = QueryBuilder.create().in("key1", "val2", "val3", "val4").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }

    @Test
    public void testQuery_in_results() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .build();

        store.save(asList(entity));

        Node node = QueryBuilder.create().and().in("key1", "val1", "val2", "val3").eq("key1", "val1").end().build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
    }

    @Test
    public void testQuery_notIn_noResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", "val1", meta))
            .build();

        store.save(asList(entity));

        Node node = QueryBuilder.create().notIn("key1", "val1", "val2", "val3").build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }

    @Test
    public void testQuery_greaterThan() {

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", 5, meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", 10, meta))
            .build();

        store.save(asList(entity, entity2));

        Node node = QueryBuilder.create().greaterThan("key1", 4).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null,DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = QueryBuilder.create().greaterThan("key1", 9).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));

        node = QueryBuilder.create().greaterThan("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }

    @Test
    public void testQuery_greaterThanEq() {


        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", 5, meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", 10, meta))
            .build();

        store.save(asList(entity, entity2));

        Node node = QueryBuilder.create().greaterThanEq("key1", 4).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = QueryBuilder.create().greaterThanEq("key1", 9).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));

        node = QueryBuilder.create().greaterThanEq("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
    }

    @Test
    public void testQuery_lessThan() {


        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", 5, meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", 10, meta))
            .build();

        store.save(asList(entity, entity2));

        Node node = QueryBuilder.create().lessThan("key1", 11).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = QueryBuilder.create().lessThan("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));

        node = QueryBuilder.create().lessThan("key1", 5).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQuery_lessThanEq() throws TableNotFoundException {


        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("key1", 5, meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("key1", 10, meta))
            .build();

        store.save(asList(entity, entity2));

        Node node = QueryBuilder.create().lessThanEq("key1", 11).build();

        Iterable<Entity> itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        node = QueryBuilder.create().lessThanEq("key1", 10).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(2, Iterables.size(itr));

        AccumuloTestUtils.dumpTable(connector, "entity_shard", new Authorizations());

        node = QueryBuilder.create().lessThanEq("key1", 5).build();

        itr = store.query(singleton("type"), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
    }

    @Test
    public void testKeys() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "1.1.1.1", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "2.2.2.2", meta))
            .build();

        Entity entity3 = EntityBuilder.create("type", "id3")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "3.3.3.3", meta))
            .build();

        store.save(asList(entity, entity2, entity3));

        CloseableIterable<Pair<String, String>> keys = store.uniqueKeys("", "type", DEFAULT_AUTHS);

        assertEquals(2, Iterables.size(keys));
        assertEquals(new Pair<String, String>("hasIp", "string"), get(keys, 0));
        assertEquals(new Pair<String, String>("ip", "string"), get(keys, 1));
    }

    @Test
    public void testExpirationOfAttributes_get() throws Exception {

        Map<String,String> expireMeta = new MetadataBuilder()
            .setExpiration(1)
            .setTimestamp(currentTimeMillis()-50000)
            .build();

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "1.1.1.1", expireMeta))
            .build();

        store.save(singleton(entity));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "entity_shard");

        List<EntityIdentifier> indexes = asList(new EntityIdentifier[] {new EntityIdentifier(entity.getType(), entity.getId())});
        Iterable<Entity> entities = store.get(indexes, null, DEFAULT_AUTHS);

        assertEquals(1, size(entities));
        assertEquals(1, get(entities, 0).size());

    }

    @Test
    public void testExpirationOfAttributes_query() throws Exception {

        Map<String,String> expireMeta = new MetadataBuilder()
            .setExpiration(1)
            .setTimestamp(currentTimeMillis()-500)
            .build();

        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "1.1.1.1", expireMeta))
            .build();

        store.save(singleton(entity));

        Node query = QueryBuilder.create().and().eq("hasIp", "true").end().build();
        Iterable<Entity> entities = store.query(newHashSet(new String[] {"type"}), query, null, DEFAULT_AUTHS);

        assertEquals(1, size(entities));
        assertEquals(1, get(entities, 0).size());

    }

    @Test
    public void testQuery_MultipleNotInQuery() throws Exception {
        Entity entity = EntityBuilder.create("type", "id")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "1.1.1.1", meta))
            .build();

        Entity entity2 = EntityBuilder.create("type", "id2")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "2.2.2.2", meta))
            .build();

        Entity entity3 = EntityBuilder.create("type", "id3")
            .attr(new Attribute("hasIp", "true", meta))
            .attr(new Attribute("ip", "3.3.3.3", meta))
            .build();

        store.save(asList(entity, entity2, entity3));

        Node query = QueryBuilder.create()
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
        Node query = QueryBuilder.create().and().end().build();

        CloseableIterable<Entity> itr = store.query(singleton("type"), query, null, DEFAULT_AUTHS);

        assertEquals(0, size(itr));
    }

    @Test
    public void test_Delete() {

        for (int i = 0; i < 10; i++) {
            Entity entity = EntityBuilder.create("type", "type_"+i)
                    .attr(new Attribute("key1", "val1", meta))
                    .attr(new Attribute("key2", "val2", meta))
                    .build();

            store.save(singleton(entity));
        }


        List<EntityIdentifier> typesAndIds = Arrays.asList(new EntityIdentifier("type", "type_0"),
                new EntityIdentifier("type", "type_1"), new EntityIdentifier("type", "type_2"));
        Iterable<Entity> actualEntities = store.get(typesAndIds, null, new Auths("A"));
        assertEquals(3,Iterables.size(actualEntities));

        store.delete(typesAndIds,new Auths("A"));

        Iterable<Entity> actualEntitiesAfterDelete = store.get(typesAndIds, null, new Auths("A"));
        assertEquals(0,Iterables.size(actualEntitiesAfterDelete));

    }

    @Test
    public void test_createAndDeleteEntities() throws Exception {

        for (int i = 0; i < 10; i++) {
            Entity entity = EntityBuilder.create("type", "type_"+i)
                    .attr(new Attribute("key1", "val1", meta))
                    .attr(new Attribute("key2", "val2", meta))
                    .build();

            store.save(singleton(entity));
        }
        store.flush();

        List<EntityIdentifier> typesAndIds = Arrays.asList(new EntityIdentifier("type", "type_0"),
                new EntityIdentifier("type", "type_1"), new EntityIdentifier("type", "type_2"));
        Iterable<Entity> actualEntities = store.get(typesAndIds, null, new Auths("A"));
        assertEquals(3,Iterables.size(actualEntities));

        store.delete(typesAndIds,new Auths("A"));
        store.flush();

        Iterable<Entity> actualEntitiesAfterDelete = store.get(typesAndIds, null, new Auths("A"));
        assertEquals(0,Iterables.size(actualEntitiesAfterDelete));

    }

    @Test
    public void test_createAndPartialDeleteBasedOnVisibilities() throws Exception {

        assertKeyValuePairsInTable(0,AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME,new Authorizations("A","B"));
        assertKeyValuePairsInTable(0,AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME,new Authorizations("A","B"));

        Entity entity = EntityBuilder.create("type", "type_0")
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta_b))
                .build();

        store.save(singleton(entity));
        store.flush();

        List<EntityIdentifier> typesAndIds = Arrays.asList(new EntityIdentifier("type", "type_0"));
        Iterable<Entity> actualEntities = store.get(typesAndIds, null, new Auths("A","B"));
        assertEquals(1,Iterables.size(actualEntities));

        assertKeyValuePairsInTable(4,AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME,new Authorizations("A","B"));
        assertKeyValuePairsInTable(5,AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME,new Authorizations("A","B"));

        //delete only visibilities I can see
        store.delete(typesAndIds,new Auths("A"));
        store.flush();

        //should be no entities
        List<Entity> actualEntitiesAfterDelete = Lists.newArrayList(store.get(typesAndIds, null, new Auths("A")));
        assertEquals(""+actualEntitiesAfterDelete,0,actualEntitiesAfterDelete.size());

        //partial entities still in tables
        assertKeyValuePairsInTable(2,AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME,new Authorizations("A","B"));
        assertKeyValuePairsInTable(2,AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME,new Authorizations("A","B"));



    }

    private void assertKeyValuePairsInTable(int expectedSize, String tableName, Authorizations authorizations) throws TableNotFoundException {
        BatchScanner batchScanner = connector.createBatchScanner(tableName,authorizations,2);
        batchScanner.setRanges(Collections.singleton(new Range((Text)null)));

        ArrayList<Map.Entry<Key, Value>> entries = Lists.newArrayList(batchScanner.iterator());

        assertEquals("size of entries not matching " +expectedSize+": "+entries + " :" + entries.size(),expectedSize, entries.size());
    }

}
