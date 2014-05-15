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

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class AccumuloEntityStoreTest {


  public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
    return new MockInstance().getConnector("root", "".getBytes());
  }

  @Test
  public void testGet() throws Exception {

    Connector connector = getConnector();
    AccumuloEntityStore store = new AccumuloEntityStore(connector);

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", "val2", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("key1", "val1", ""));
    entity2.put(new Tuple("key2", "val2", ""));

    store.save(asList(entity, entity2));

    Scanner scanner = connector.createScanner("entityStore_shard", new Authorizations());
    for(Map.Entry<Key,Value> entry : scanner) {
      System.out.println(entry);
    }

    CloseableIterable<Entity> actualEntity = store.get(singletonList(new EntityIndex("type", "id")), null, new Auths());

    assertEquals(1, Iterables.size(actualEntity));
    Entity actual = actualEntity.iterator().next();
    assertEquals(new HashSet(actual.getTuples()), new HashSet(entity.getTuples()));
    assertEquals(actual.getId(), entity.getId());
    assertEquals(actual.getType(), entity.getType());
  }

  @Test
  public void testGet_withSelection() throws Exception {

    Connector connector = getConnector();
    AccumuloEntityStore store = new AccumuloEntityStore(connector);

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", "val2", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("key1", "val1", ""));
    entity2.put(new Tuple("key2", "val2", ""));

    store.save(asList(entity, entity2));

    CloseableIterable<Entity> actualEvent = store.get(singletonList(new EntityIndex("type", entity.getId())),
            singleton("key1"), new Auths());

    assertEquals(1, Iterables.size(actualEvent));
    assertNull(actualEvent.iterator().next().get("key2"));
    assertNotNull(actualEvent.iterator().next().get("key1"));
  }


  @Test
  public void testQuery_withSelection() throws Exception {
    AccumuloEntityStore store = new AccumuloEntityStore(getConnector());

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", "val2", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("key1", "val1", ""));
    entity2.put(new Tuple("key2", "val2", ""));

    store.save(asList(entity, entity2));

    Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").end().build();

    Iterable<Entity> itr = store.query(singleton("type"), query, singleton("key1"), new Auths());

    int count = 0;
    for(Entity entry : itr) {
      count++;
      assertNull(entry.get("key2"));
      assertNotNull(entry.get("key1"));
    }
    assertEquals(2, count);
  }

  @Test
  public void testQuery_AndQuery() throws Exception {
    AccumuloEntityStore store = new AccumuloEntityStore(getConnector());

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", "val2", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("key1", "val1", ""));
    entity2.put(new Tuple("key2", "val2", ""));

    store.save(asList(entity, entity2));

    Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").end().build();

    Iterator<Entity> itr = store.query(singleton("type"), query, null, new Auths()).iterator();

    Entity actualEvent = itr.next();
    if(actualEvent.getId().equals(entity.getId())) {
      assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity.getTuples()));
    }

    else {
      assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity2.getTuples()));
    }

    actualEvent = itr.next();
    if(actualEvent.getId().equals(entity.getId())) {
      assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity.getTuples()));
    }

    else {
      assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(entity2.getTuples()));
    }
  }

  @Test
  public void testQuery_OrQuery() throws Exception {
    AccumuloEntityStore store = new AccumuloEntityStore(getConnector());

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", "val2", ""));

    Entity entity2 = new BaseEntity("type", "id3");
    entity2.put(new Tuple("key1", "val1", ""));
    entity2.put(new Tuple("key3", "val3", ""));

    store.save(asList(entity, entity2));

    Node query = new QueryBuilder().or().eq("key3", "val3").eq("key2", "val2").end().build();

    Iterator<Entity> itr = store.query(singleton("type"), query, null, new Auths()).iterator();

    Entity actualEvent = itr.next();
    if(actualEvent.getId().equals(entity.getId())) {
      assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
    }
    else {
      assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));
    }

    actualEvent = itr.next();
    if(actualEvent.getId().equals(entity.getId())) {
      assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
    }
    else {
      assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));
    }
  }

  @Test
  public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {
    AccumuloEntityStore store = new AccumuloEntityStore(getConnector());

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", "val2", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("key1", "val1", ""));
    entity2.put(new Tuple("key3", "val3", ""));

    store.save(asList(entity, entity2));

    Node query = new QueryBuilder().eq("key1", "val1").build();

    Iterator<Entity> itr = store.query(singleton("type"), query, null, new Auths()).iterator();

    Entity actualEvent = itr.next();
    if(actualEvent.getId().equals(entity.getId())) {
      assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
    }

    else {
      assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));
    }

    actualEvent = itr.next();
    if(actualEvent.getId().equals(entity.getId())) {
      assertEquals(new HashSet(entity.getTuples()), new HashSet(actualEvent.getTuples()));
    }

    else {
      assertEquals(new HashSet(entity2.getTuples()), new HashSet(actualEvent.getTuples()));
    }
  }

  @Test
  public void testKeys() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    AccumuloEntityStore store = new AccumuloEntityStore(getConnector());

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("hasIp", "true", ""));
    entity.put(new Tuple("ip", "1.1.1.1", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("hasIp", "true", ""));
    entity2.put(new Tuple("ip", "2.2.2.2", ""));

    Entity entity3 = new BaseEntity("type", "id3");
    entity3.put(new Tuple("hasIp", "true", ""));
    entity3.put(new Tuple("ip", "3.3.3.3", ""));

    store.save(asList(entity, entity2, entity3));

    CloseableIterable<Pair<String,String>> keys = store.keys("type", new Auths());

    assertEquals(2, Iterables.size(keys));
    assertEquals(new Pair<String,String>("hasIp", "string"), Iterables.get(keys, 0));
    assertEquals(new Pair<String,String>("ip", "string"), Iterables.get(keys, 1));
  }

  @Test
  public void testQuery_MultipleNotInQuery() throws Exception {
    AccumuloEntityStore store = new AccumuloEntityStore(getConnector());

    Entity entity = new BaseEntity("type", "id");
    entity.put(new Tuple("hasIp", "true", ""));
    entity.put(new Tuple("ip", "1.1.1.1", ""));

    Entity entity2 = new BaseEntity("type", "id2");
    entity2.put(new Tuple("hasIp", "true", ""));
    entity2.put(new Tuple("ip", "2.2.2.2", ""));

    Entity entity3 = new BaseEntity("type", "id3");
    entity3.put(new Tuple("hasIp", "true", ""));
    entity3.put(new Tuple("ip", "3.3.3.3", ""));

    store.save(asList(entity, entity2, entity3));

    Node query = new QueryBuilder()
            .and()
            .notEq("ip", "1.1.1.1")
            .notEq("ip", "2.2.2.2")
            .notEq("ip", "4.4.4.4")
            .eq("hasIp", "true")
            .end().build();

    Iterator<Entity> itr = store.query(singleton("type"), query, null, new Auths()).iterator();

    int x = 0;

    while (itr.hasNext()) {
      x++;
      Entity e = itr.next();
      assertEquals("3.3.3.3",e.get("ip").getValue());

    }
    assertEquals(1, x);
  }

}
