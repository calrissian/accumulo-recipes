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
package org.calrissian.accumulorecipes.lastn;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.domain.event.Event;

/**
 * The LastN store is a version-based eviction mechanism- meaning that it will only keep around the last N versions of
 * an indexed set of attributes but it will maintain cell-level security of those attributes. This is useful in news
 * feeds and places where it's important to know the 'most recent' history of something.
 */
public interface LastNStore {

    /**
     * Puts a StoreEntry into the Last N store under the specified grouping. The Last N items returned are all grouped
     * by the group value.
     *
     * @param group
     * @param entry
     */
    void put(String group, Event entry);

    void flush() throws Exception;

    /**
     * Returns the last N store entries under the specified group- starting with the most recent.
     *
     * @param group
     * @param auths
     * @return
     */
    Iterable<Event> get(String group, Auths auths);

}
