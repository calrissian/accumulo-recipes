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
package org.calrissian.accumulorecipes.eventstore;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventIndex;

/**
 * An event store generally holds temporal keys/values.
 */
public interface EventStore {

    /**
     * Persists a set of StoreEntry objects into the event store
     *
     * @param events
     * @throws Exception
     */
    void save(Iterable<? extends Event> events);

    void flush() throws Exception;

    /**
     * Query the store using criteria specified with the specified filter fields
     *
     * @param start
     * @param end
     * @param node
     * @param auths
     * @return
     */
    CloseableIterable<Event> query(Date start, Date end, Node node, Set<String> selectFields, Auths auths);

    /**
     * Query the store using criteria specified
     *
     * @param start
     * @param end
     * @param node
     * @param auths
     * @return
     */
    CloseableIterable<Event> query(Date start, Date end, Node node, Auths auths);

    /**
     * If an event is already being indexed in another store, it's often useful to query a bunch
     * back in batches. This method allows the selection of specific fields.
     *
     * @param indexes
     * @param auths
     * @return
     */
    CloseableIterable<Event> get(Collection<EventIndex> indexes, Set<String> selectFields, Auths auths);

    /**
     * Queries events back by id and timestamp.
     * @param indexes
     * @param auths
     * @return
     */
    CloseableIterable<Event> get(Collection<EventIndex> indexes, Auths auths);
}
