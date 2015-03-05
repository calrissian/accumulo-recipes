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

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.size;
import static com.google.common.io.Resources.getResource;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Collections.sort;
import static org.calrissian.mango.json.util.store.JsonTupleStore.fromJson;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Date;
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
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.json.util.store.JsonTupleStore.FlattenedLevelsComparator;
import org.junit.Before;
import org.junit.Test;

/**
 * A real-world example to test storage/query of twitter json.
 * @throws Exception
 */
public class JsonEventStoreTest {

    private static FlattenedLevelsComparator comparator = new FlattenedLevelsComparator();

    private EventStore store;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testTwitterJson() throws Exception {

        List<Event> eventList = new ArrayList<Event>();

        /**
         * First, we'll load a json object representing tweets
         */
        String tweetJson = Resources.toString(getResource("twitter_tweets.json"), defaultCharset());

        /**
         * Create tweet event with a random UUID and timestamp of current time
         * (both of these can be set manually in the constructor)
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

        AccumuloTestUtils.dumpTable(getConnector(), "eventStore_shard");

        /**
         * Build our query to retrieve stored events by their flattened json
         * representation.
         */
        Node query = new QueryBuilder()
            .and()
                .eq("statuses_$entities_$hashtags_$indices", 29)     // the json tree has been flattened
                .eq("statuses_$user_$name", "Sean Cummings")        // into key/value objects
            .end()
        .build();

        CloseableIterable<Event> results = store.query(
            new Date(0),
            new Date(currentTimeMillis()),
            query,
            new Auths()
        );

        assertEquals(1, size(results));

        List<Attribute> expectedAttributes = new ArrayList(tweetEvent.getAttributes());
        List<Attribute> actualAttributes = new ArrayList<Attribute>(get(results, 0).getAttributes());
        sort(expectedAttributes, comparator);
        sort(actualAttributes, comparator);

        assertEquals(expectedAttributes, actualAttributes);
    }

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance("jsonTest").getConnector("root", "".getBytes());
    }

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        store = new AccumuloEventStore(getConnector());
    }


}
