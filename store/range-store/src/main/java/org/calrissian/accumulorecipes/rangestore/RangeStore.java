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
package org.calrissian.accumulorecipes.rangestore;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.mango.types.range.ValueRange;

/**
 * A range store is a 1-dimensional NoSQL key/value version of the common interval tree data structure.
 */
public interface RangeStore<T extends Comparable<T>> {

    /**
     * Inserts ranges into the store.
     * @param ranges
     */
    void save(Iterable<ValueRange<T>> ranges);

    /**
     * Deletes ranges from the store.
     * @param ranges
     */
    void delete(Iterable<ValueRange<T>> ranges);

    /**
     * Queries for any ranges that intersect, overlap, or are contained by the given range.
     * @param range
     * @param auths
     * @return
     */
    Iterable<ValueRange<T>> query(ValueRange<T> range, Authorizations auths);

}
