/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.eventstore.impl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.json.util.json.JsonTupleStore;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_STORE_CONFIG;
import static org.calrissian.mango.json.util.json.JsonTupleStore.fromJson;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
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
    store = new AccumuloEventStore(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_STORE_CONFIG, LEXI_TYPES, new HourlyShardBuilder(25));
  }


  /**
   * A real-world example to test storage/query of twitter json
   * @throws Exception
   */
  @Test
  public void testTwitterJson() throws Exception {

    List<Event> eventList = new ArrayList<Event>();

    /**
     * First, we'll load a json object representing tweets
     */
    String tweetJson = loadResourceAsString("twitter_tweets.json");

    /**
     * Create tweet event
     */
    Event tweetEvent = new BaseEvent();
    tweetEvent.putAll(fromJson(tweetJson, objectMapper));

    eventList.add(tweetEvent);

    /**
     * Next, we'll load a json array containing user timeline data
     */
    String timelineJson = loadResourceAsString("twitter_timeline.json");

    /**
     * Since we need to persist objects, we'll loop through the array and create
     * events out of the objects
     */
    ArrayNode node = (ArrayNode) objectMapper.readTree(timelineJson);
    for(JsonNode node1 : node) {

      // create an event from the current json object
      Event timelineEvent = new BaseEvent();
      timelineEvent.putAll(fromJson((ObjectNode) node1));

      eventList.add(timelineEvent);
    }

    /**
     * Save events in the event store and flush
     */
    store.save(eventList);
    store.flush();

    /**
     * Build our query to retrieve stored events by their flattened json
     * representation.
     */
    Node query = new QueryBuilder()
        .and()
          .eq("statuses$entities$hashtags$indices", 29)
          .eq("statuses$user$name", "Sean Cummings")
        .end()
      .build();

    CloseableIterable<Event> results = store.query(
        new Date(0),
        new Date(System.currentTimeMillis() + 5000),
        query,
        null,
        new Auths()
    );

    assertEquals(1, size(results));
    assertEquals(new HashSet<Tuple>(tweetEvent.getTuples()),
                 new HashSet<Tuple>(get(results, 0).getTuples())
    );
  }

  @Test
  public void test() throws Exception {

    // Nested json
    String json = "{ \"name\":\"Corey\", \"nestedObject\":{\"anotherNest\":{\"innerObj\":\"innerVal\"}}, \"nestedArray\":[\"2\",[[\"4\"],[\"1\"],[\"1\"], \"7\"]], \"ids\":[\"5\",\"2\"], \"locations\":[{\"name\":\"Office\", \"addresses\":[{\"number\":1234,\"street\":\"BlahBlah Lane\"}]}]}";

    // Create event from json
    Event event = new BaseEvent();
    event.putAll(fromJson(json, objectMapper));

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

    CloseableIterable<Event> results = store.query(
        new Date(currentTimeMillis()-5000),
        new Date(),
        query,
        null,
        new Auths()
    );

    assertEquals(1, size(results));

    for(Event even : results) {
      System.out.println(even);
      System.out.println(JsonTupleStore.toJsonString(even.getTuples(), objectMapper));
    }
  }

  private String loadResourceAsString(String resource) throws IOException {

    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    BufferedInputStream bif = new BufferedInputStream(inputStream);
    BufferedReader reader = new BufferedReader(new InputStreamReader(bif));

    String string = "";
    String line;
    while((line = reader.readLine()) != null)
      string+=line;

    return string;
  }
}
