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
package org.calrissian.accumulorecipes.entitystore;


import java.util.List;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIndex;

/**
 * An entity store is for objects that represent elements of the real world. Entities can have
 * first-class relationships to other entities.
 */
public interface EntityStore {

    /**
     * Saves entities to the underlying storage implementation
     * @param entities
     */
    void save(Iterable<? extends Entity> entities);

    /**
     * Retrieves a list of entities by their types and ids. This method also allows for selection
     * of specific subsets of fields. Only attributes that match the given auths will be returned.
     * @param typesAndIds
     * @param selectFields
     * @param auths
     * @return
     */
    CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths);

    /**
     * Retrives a list of entities by their types and ids. Only attributes with the given auths will
     * be returned.
     * @param typesAndIds
     * @param auths
     * @return
     */
    CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Auths auths);

    /**
     * Retrieves all entities for a specified type. Only fields in the given set of select fields will
     * be returned for each entity (null will return all fields). Only attributes matching the given
     * auths will be included in the resulting entities.
     * @param types
     * @param selectFields
     * @param auths
     * @return
     */
    CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths);

    /**
     * Retrieves all entities for the specified types. Only attributes matching the given auths will be
     * included in the resulting entities.
     * @param types
     * @param auths
     * @return
     */
    CloseableIterable<Entity> getAllByType(Set<String> types, Auths auths);

    /**
     * Retrieves all entities for the specified types that match the given query. Only fields included in the
     * set of select fields will be returned for each entity. Only attributes matching the given auths will
     * be included in the resulting entities.
     * @param types
     * @param query
     * @param selectFields
     * @param auths
     * @return
     */
    CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths);

    /**
     * Retrives all entities for the specified types that match the given query. Only attributes matching the
     * given auths will be included in the resulting entities.
     * @param types
     * @param query
     * @param auths
     * @return
     */
    CloseableIterable<Entity> query(Set<String> types, Node query, Auths auths);

    /**
     * Retrieves all the keys for the specified entity type. Keys are represented by a pair. The first
     * item in the pair is the name of the key. The second item in the pair is the datatype. It's possible
     * that if there are multiple values for the same key with different datatypes that multiple Pairs could
     * be returned for the same key.
     * @param type
     * @param auths
     * @return
     */
    CloseableIterable<Pair<String, String>> keys(String type, Auths auths);

    /**
     * Flushes the in-memory buffer of entities to the server. It's important to make sure method is eventually
     * called or data loss could occur.
     * @throws Exception
     */
    void flush() throws Exception;

    /**
     * Frees up resources and shuts down the entity store.
     * @throws Exception
     */
    void shutdown() throws Exception;
}
