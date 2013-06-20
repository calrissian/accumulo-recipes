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

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;

import java.util.Collection;
import java.util.Date;

/**
 * An event store generally holds temporal keys/values.
 */
public interface EventStore {

    /**
     * Persists a collection of StoreEntry objects into the event store
     * @param events
     * @throws Exception
     */
    void put(Collection<StoreEntry> events) throws Exception;

    /**
     * Shut down the store and cleanup any resources being held
     * @throws Exception
     */
    void shutdown() throws Exception;

    /**
     * Query the store using criteria specified
     * @param start
     * @param end
     * @param node
     * @param auths
     * @return
     */
    CloseableIterable<StoreEntry> query(Date start, Date end, Node node, Authorizations auths);

    /**
     * Get a specific StoreEntry with the given ID
     * @param uuid
     * @param auths
     * @return
     */
    StoreEntry get(String uuid, Authorizations auths);
}
