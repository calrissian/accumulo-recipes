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
package org.calrissian.accumulorecipes.commons.collect;

import com.google.common.collect.Iterables;
import org.calrissian.mango.domain.BaseEvent;
import org.calrissian.mango.domain.Event;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

public class EventMergeJoinIterableTest {

    @Test
    public void test() {

        Event entry1 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 5000);
        Event entry2 = new BaseEvent(randomUUID().toString(), currentTimeMillis());
        Event entry3 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 30);

        List<Iterable<Event>> entryIter = new LinkedList<Iterable<Event>>();
        entryIter.add(Arrays.asList(new Event[]{entry1}));
        entryIter.add(Arrays.asList(new Event[]{entry2}));
        entryIter.add(Arrays.asList(new Event[]{entry3}));

        EventMergeJoinIterable iterable = new EventMergeJoinIterable(entryIter);

        assertEquals(3, Iterables.size(iterable));
        assertEquals(entry2, Iterables.get(iterable, 0));
        assertEquals(entry3, Iterables.get(iterable, 1));
        assertEquals(entry1, Iterables.get(iterable, 2));

    }
}
