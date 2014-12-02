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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
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
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.google.common.io.Resources.getResource;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_STORE_CONFIG;
import static org.calrissian.mango.json.util.json.JsonTupleStore.fromJson;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;

/**
 * A real-world example to test storage/query of twitter json.
 * @throws Exception
 */
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

  @Test
  public void testTwitterJson() throws Exception {

    List<Event> eventList = new ArrayList<Event>();

    /**
     * First, we'll load a json object representing tweets
     */
    String tweetJson = Resources.toString(getResource("twitter_tweets.json"), defaultCharset());

    /**
     * Create tweet event
     */
    Event tweetEvent = new BaseEvent();
    tweetEvent.putAll(fromJson(tweetJson, objectMapper));

    eventList.add(tweetEvent);

    /**
     * Next, we'll load a json array containing user timeline data
     */
    String timelineJson = Resources.toString(getResource("twitter_timeline.json"), defaultCharset());

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
          .eq("statuses$entities$hashtags$indices", 29)     // the json tree has been flattened
          .eq("statuses$user$name", "Sean Cummings")        // into key/value objects
        .end()
      .build();

    CloseableIterable<Event> results = store.query(
        new Date(0),
        new Date(currentTimeMillis() + 5000),
        query,
        new Auths()
    );

    assertEquals(1, size(results));
    assertEquals(new HashSet<Tuple>(tweetEvent.getTuples()),
                 new HashSet<Tuple>(get(results, 0).getTuples())
    );
  }

}
