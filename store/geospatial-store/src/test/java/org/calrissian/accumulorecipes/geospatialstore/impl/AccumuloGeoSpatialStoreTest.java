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
package org.calrissian.accumulorecipes.geospatialstore.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccumuloGeoSpatialStoreTest {

    Connector connector;
    AccumuloGeoSpatialStore store;

    @Before
    public void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "".getBytes());

        store = new AccumuloGeoSpatialStore(connector);
    }

    @Test
    public void test_singleEntryReturns() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Event entry = new BaseEvent();
        entry.put(new Tuple("Key1", "Val1", ""));
        entry.put(new Tuple("key2", "val2", ""));

        store.put(singleton(entry), new Point2D.Double(-1, 1));

        CloseableIterable<Event> entries = store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0, 2.0), new Auths());
        assertEquals(1, Iterables.size(entries));
        assertEquals(entry, Iterables.get(entries, 0));
    }

    @Test
    public void test_singleEntryReturns_withMultipleEntriesInStore() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Event entry = new BaseEvent();
        entry.put(new Tuple("Key1", "Val1", ""));
        entry.put(new Tuple("key2", "val2", ""));

        Event entry2 = new BaseEvent();
        entry2.put(new Tuple("Key1", "Val1", ""));
        entry2.put(new Tuple("key2", "val2", ""));


        store.put(singleton(entry), new Point2D.Double(-1, 1));
        store.put(singleton(entry2), new Point2D.Double(-5, 1));

        CloseableIterable<Event> entries = store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0, 2.0), new Auths());
        assertEquals(1, Iterables.size(entries));
        assertEquals(entry, Iterables.get(entries, 0));
    }

    @Test
    public void test_multipleEntriesReturn_withMultipleEntriesInStore() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Event entry = new BaseEvent();
        entry.put(new Tuple("Key1", "Val1", ""));
        entry.put(new Tuple("key2", "val2", ""));

        Event entry2 = new BaseEvent();
        entry2.put(new Tuple("Key1", "Val1", ""));
        entry2.put(new Tuple("key2", "val2", ""));

        Event entry3 = new BaseEvent();
        entry3.put(new Tuple("Key1", "Val1", ""));
        entry3.put(new Tuple("key2", "val2", ""));


        store.put(singleton(entry), new Point2D.Double(-1, 1));
        store.put(singleton(entry2), new Point2D.Double(1, 1));
        store.put(singleton(entry3), new Point2D.Double(1, -1));

        CloseableIterable<Event> entries = store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0, 2.0), new Auths());
        assertEquals(3, Iterables.size(entries));

        Event actualEntry1 = Iterables.get(entries, 0);
        assertTrue(actualEntry1.equals(entry) || actualEntry1.equals(entry2) || actualEntry1.equals(entry3));

        Event actualEntry2 = Iterables.get(entries, 0);
        assertTrue(actualEntry2.equals(entry) || actualEntry2.equals(entry2) || actualEntry2.equals(entry3));

        Event actualEntry3 = Iterables.get(entries, 0);
        assertTrue(actualEntry3.equals(entry) || actualEntry3.equals(entry2) || actualEntry3.equals(entry3));

    }

}
