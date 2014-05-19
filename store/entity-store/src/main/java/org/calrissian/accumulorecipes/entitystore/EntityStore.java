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


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Pair;

import java.util.List;
import java.util.Set;

public interface EntityStore {

    void save(Iterable<Entity> entities);

    CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths);

    CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths);

    CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths);

    CloseableIterable<Pair<String, String>> keys(String type, Auths auths);

    void delete(Iterable<EntityIndex> typesAndIds, Auths auths);

    void shutdown() throws Exception;
}
