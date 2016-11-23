/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.store.json.event.impl;

import static com.google.common.collect.Iterables.transform;
import static org.apache.commons.lang.StringUtils.replace;
import static org.calrissian.mango.json.util.store.JsonAttributeStore.fromMap;
import static org.calrissian.mango.json.util.store.JsonAttributeStore.toObject;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.calrissian.mango.domain.event.EventIdentifier;
import org.calrissian.store.json.event.JsonEvent;
import org.calrissian.store.json.event.JsonEventStore;

public class AccumuloJsonEventStore implements JsonEventStore {

    private EventStore eventStore;

    public AccumuloJsonEventStore(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public void save(Iterable<JsonEvent> jsonEvents) {
        eventStore.save(transform(jsonEvents, jsonEventEventFunction));
    }

    @Override
    public void flush() throws Exception {
        eventStore.flush();
    }

    @Override
    public CloseableIterable<JsonEvent> query(Date start, Date end, Node node, Set<String> selectFields, Auths auths) {
        return CloseableIterables.transform(eventStore.query(start, end, node, selectFields, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> query(Date start, Date end, Set<String> types, Node node, Set<String> selectFields, Auths auths) {
        return CloseableIterables.transform(eventStore.query(start, end, types, node, selectFields, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> query(Date start, Date end, Node node, Auths auths) {
        return CloseableIterables.transform(eventStore.query(start, end, node, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> query(Date start, Date end, Set<String> types, Node node, Auths auths) {
        return CloseableIterables.transform(eventStore.query(start, end, types, node, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> get(Collection<EventIdentifier> indexes, Set<String> selectFields, Auths auths) {
        return CloseableIterables.transform(eventStore.get(indexes, selectFields, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> getAllByType(Date start, Date stop, Set<String> types, Set<String> selectFields, Auths auths) {
        return CloseableIterables.transform(eventStore.getAllByType(start, stop, types, selectFields, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> getAllByType(Date start, Date stop, Set<String> types, Auths auths) {
        return CloseableIterables.transform(eventStore.getAllByType(start, stop, types, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<JsonEvent> get(Collection<EventIdentifier> indexes, Auths auths) {
        return CloseableIterables.transform(eventStore.get(indexes, auths), eventJsonEventFunction);
    }

    @Override
    public CloseableIterable<Pair<String,String>> uniqueKeys(String prefix, String type, Auths auths) {
        return CloseableIterables.transform(eventStore.uniqueKeys(prefix, type, auths), flattenedKeyToDottedKey);
    }

    @Override
    public CloseableIterable<Object> uniqueValuesForKey(String prefix, String type, String alias, String key, Auths auths) {
        return eventStore.uniqueValuesForKey(prefix, type, alias, key, auths);
    }

    @Override
    public CloseableIterable<String> getTypes(String prefix, Auths auths) {
        return eventStore.getTypes(prefix, auths);
    }

    private static Function<JsonEvent,Event> jsonEventEventFunction = new Function<JsonEvent,Event>() {
        @Override
        public Event apply(JsonEvent jsonEvent) {
            EventBuilder eventBuilder = EventBuilder.create(jsonEvent.getType(), jsonEvent.getId(), jsonEvent.getTimestamp());
            eventBuilder.attrs(fromMap(jsonEvent.getDocument()));
            return eventBuilder.build();
        }
    };

    private static Function<Event,JsonEvent> eventJsonEventFunction = new Function<Event,JsonEvent>() {
        @Override
        public JsonEvent apply(Event event) {
            Map<String, Object> attrs = toObject(event.getAttributes());
            return new JsonEvent(event.getType(), event.getId(), event.getTimestamp(), attrs);
        }
    };

    private static Function<Pair<String, String>, Pair<String, String>> flattenedKeyToDottedKey = new Function<Pair<String, String>, Pair<String, String>>() {
        @Override
        public Pair<String, String> apply(Pair<String, String> s) {
            return new Pair<String,String>(s.getOne(), replace(s.getTwo(), "_$", "."));
        }
    };

}
