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
package org.calrissian.accumulorecipes.temporal.lastn.support;

import com.google.common.collect.Iterables;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventIdentifier;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

public class EventMergeJoinIterableTest {

    @Test
    public void test() {

        Event entry1 = new BaseEvent(new EventIdentifier("", randomUUID().toString(), currentTimeMillis() - 5000), Collections.<Attribute>emptySet());
        Event entry2 = new BaseEvent(new EventIdentifier("", randomUUID().toString(), currentTimeMillis()), Collections.<Attribute>emptySet());
        Event entry3 = new BaseEvent(new EventIdentifier("", randomUUID().toString(), currentTimeMillis() - 30), Collections.<Attribute>emptySet());

        List<Iterable<Event>> entryIter = new LinkedList<Iterable<Event>>();
        entryIter.add(singletonList(entry1));
        entryIter.add(singletonList(entry2));
        entryIter.add(singletonList(entry3));

        EventMergeJoinIterable iterable = new EventMergeJoinIterable(entryIter);

        assertEquals(3, Iterables.size(iterable));
        assertEquals(entry2, Iterables.get(iterable, 0));
        assertEquals(entry3, Iterables.get(iterable, 1));
        assertEquals(entry1, Iterables.get(iterable, 2));

    }
}
