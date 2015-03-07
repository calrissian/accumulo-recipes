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
package org.calrissian.accumulorecipes.temporal.lastn;

import java.util.Date;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.event.Event;

/**
 * The temporal last n store will return the last n events in some series of groups
 * for some range of time. It's especially useful for logs and news feeds as it gives
 * the ability to zero-in on a window of those events and, in the case where multiple groups
 * are provided to the get method, provides a holistic view of the events in the those
 * groups.
 */
public interface TemporalLastNStore {

    /**
     * Puts an event into the store under the specified group.
     */
    void put(String group, Event entry);

    void flush() throws Exception;

    /**
     * Gets the last-n events from the store in a holistic view of the specified groups
     * for the specified time range.
     */
    CloseableIterable<Event> get(Date start, Date stop, Set<String> groups, int n, Auths auths);
}
