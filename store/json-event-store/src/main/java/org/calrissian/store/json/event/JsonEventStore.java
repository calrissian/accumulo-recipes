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
package org.calrissian.store.json.event;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.event.EventIdentifier;

public interface JsonEventStore {

    void save(Iterable<JsonEvent> jsonEvents);

    /**
     * Force persistence of all events currently in-memory to the backing persistence
     * implementation.
     * @throws Exception
     */
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
    @Deprecated
    CloseableIterable<JsonEvent> query(Date start, Date end, Node node, Set<String> selectFields, Auths auths);


    CloseableIterable<JsonEvent> query(Date start, Date end, Set<String> types, Node node, Set<String> selectFields, Auths auths);

    /**
     * Query the store using criteria specified
     *
     * @param start
     * @param end
     * @param node
     * @param auths
     * @return
     */
    @Deprecated
    CloseableIterable<JsonEvent> query(Date start, Date end, Node node, Auths auths);


    CloseableIterable<JsonEvent> query(Date start, Date end, Set<String> types, Node node, Auths auths);

    /**
     * If an event is already being indexed in another store, it's often useful to query a bunch
     * back in batches. This method allows the selection of specific fields.
     *
     * @param indexes
     * @param auths
     * @return
     */
    CloseableIterable<JsonEvent> get(Collection<EventIdentifier> indexes, Set<String> selectFields, Auths auths);

    CloseableIterable<JsonEvent> getAllByType(Date start, Date stop, Set<String> types, Set<String> selectFields, Auths auths);

    CloseableIterable<JsonEvent> getAllByType(Date start, Date stop, Set<String> types, Auths auths);

    /**
     * Queries events back by id and timestamp.
     * @param indexes
     * @param auths
     * @return
     */
    CloseableIterable<JsonEvent> get(Collection<EventIdentifier> indexes, Auths auths);


    public CloseableIterable<Pair<String,String>> uniqueKeys(String prefix, String type, Auths auths);
    public CloseableIterable<Object> uniqueValuesForKey(String prefix, String type, String alias, String key, Auths auths);
    public CloseableIterable<String> getTypes(String prefix, Auths auths);
}
