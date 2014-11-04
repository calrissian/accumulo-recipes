package org.calrissian.accumulorecipes.eventstore.impl;

import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.json.util.json.JsonTupleStore;
import org.junit.Before;
import org.junit.Test;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class JsonEventStoreTest {

  private Connector connector;
  private EventStore store;
  private ObjectMapper objectMapper = new ObjectMapper();

  public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
    return new MockInstance().getConnector("root", "".getBytes());
  }

  @Before
  public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    connector = getConnector();
    store = new AccumuloEventStore(connector);
  }

  @Test
  public void test() throws Exception {

    // Nested json
    String json = "{ \"name\":\"Corey\", \"nestedObject\":{\"anotherNest\":{\"innerObj\":\"innerVal\"}}, \"nestedArray\":[\"2\",[[\"4\"],[\"1\"],[\"1\"], \"7\"]], \"ids\":[\"5\",\"2\"], \"locations\":[{\"name\":\"Office\", \"addresses\":[{\"number\":1234,\"street\":\"BlahBlah Lane\"}]}]}";

    // Create event from json
    Event event = new BaseEvent();
    event.putAll(JsonTupleStore.fromJson(json, objectMapper));
    System.out.println(event);

    // Persist event
    store.save(singleton(event));
    store.flush();

    Node query = new QueryBuilder()
      .and()
        .eq("name", "Corey")
        .eq("nestedObject$anotherNest$innerObj", "innerVal")
        .eq("nestedArray", "2")
      .end()
    .build();

    CloseableIterable<Event> results = store.query(new Date(currentTimeMillis()-5000), new Date(), query, null, new Auths());

    assertEquals(1, Iterables.size(results));

    for(Event even : results) {

      System.out.println(even);

      System.out.println(JsonTupleStore.toJsonString(even.getTuples(), objectMapper));
    }
  }
}
