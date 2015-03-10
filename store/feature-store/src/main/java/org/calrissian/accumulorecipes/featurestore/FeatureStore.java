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
package org.calrissian.accumulorecipes.featurestore;

import java.util.Date;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.Feature;
import org.calrissian.mango.collect.CloseableIterable;

public interface FeatureStore {

    /**
     * Save a set of features to the store
     */
    void save(Iterable<? extends Feature> featureData);

    /**
     * Save a set of features to the store for the specified time units
     */
    void save(Iterable<? extends Feature> features, Iterable<TimeUnit> timeUnits);

    void flush() throws Exception;

    /**
     * Query features back from the store.
     */
    <T extends Feature> CloseableIterable<T> query(Date start, Date end, String group, String type,
                                                   String name, TimeUnit timeUnit, Class<T> featureType, Auths auths);


    /**
     * Queries features back from the store for a group and multiple types
     */
    <T extends Feature> CloseableIterable<T> query(Date start, Date end, String group, Set<String> types,
                                                   String name, TimeUnit timeUnit, Class<T> featureType, Auths auths);

    /**
     * Returns all unique groups from the index. Only groups starting with the given prefix will be returned.
     * An empty prefix will return all groups.
     * @param prefix
     * @param auths
     * @return
     */
    Iterable<String> groups(String prefix, Auths auths);

    /**
     * Returns all unique types from the index for a given group. Only types starting with the given prefix
     * will be returned. An empty prefix will return all types in the group.
     * @param group
     * @param prefix
     * @param auths
     * @return
     */
    Iterable<String> types(String group, String prefix, Auths auths);

    /**
     * Returns all unique names for a given group and type. Only names starting with the given prefix
     * will be returned. An empty prefix will return all names for the given group and type.
     * @param typeAndGroup
     * @param prefix
     * @param auths
     * @return
     */
    Iterable<String> names(String group, String type, String prefix, Auths auths);
}
