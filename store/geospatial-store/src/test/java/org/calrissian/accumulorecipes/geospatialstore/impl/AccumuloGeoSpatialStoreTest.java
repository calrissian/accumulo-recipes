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

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.UUID;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Before;
import org.junit.Test;

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

        Entity entry = EntityBuilder.create("type1", UUID.randomUUID().toString())
            .attr(new Attribute("Key1", "Val1"))
            .attr(new Attribute("key2", "val2"))
            .build();

        store.put(singleton(entry), new Point2D.Double(-1, 1));

        CloseableIterable<Entity> entries = store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0, 2.0), Sets.newHashSet("type1"), Auths.EMPTY);
        assertEquals(1, Iterables.size(entries));
        assertEquals(entry, Iterables.get(entries, 0));
    }

    @Test
    public void test_singleEntryReturns_withMultipleEntriesInStore() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entry = EntityBuilder.create("type1", UUID.randomUUID().toString())
            .attr(new Attribute("Key1", "Val1"))
            .attr(new Attribute("key2", "val2"))
            .build();

        Entity entry2 = EntityBuilder.create("type1", UUID.randomUUID().toString())
            .attr(new Attribute("Key1", "Val1"))
            .attr(new Attribute("key2", "val2"))
            .build();


        store.put(singleton(entry), new Point2D.Double(-1, 1));
        store.put(singleton(entry2), new Point2D.Double(-5, 1));

        CloseableIterable<Entity> entries = store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0, 2.0), Sets.newHashSet("type1"), Auths.EMPTY);
        assertEquals(1, Iterables.size(entries));
        assertEquals(entry, Iterables.get(entries, 0));
    }

    @Test
    public void test_multipleEntriesReturn_withMultipleEntriesInStore() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Entity entry = EntityBuilder.create("type1", UUID.randomUUID().toString())
            .attr(new Attribute("Key1", "Val1"))
            .attr(new Attribute("key2", "val2"))
            .build();

        Entity entry2 = EntityBuilder.create("type1", UUID.randomUUID().toString())
            .attr(new Attribute("Key1", "Val1"))
            .attr(new Attribute("key2", "val2"))
            .build();

        Entity entry3 = EntityBuilder.create("type1", UUID.randomUUID().toString())
            .attr(new Attribute("Key1", "Val1"))
            .attr(new Attribute("key2", "val2"))
            .build();



        store.put(singleton(entry), new Point2D.Double(-1, 1));
        store.put(singleton(entry2), new Point2D.Double(1, 1));
        store.put(singleton(entry3), new Point2D.Double(1, -1));

        CloseableIterable<Entity> entries = store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0, 2.0), Sets.newHashSet("type1"), Auths.EMPTY);
        assertEquals(3, Iterables.size(entries));

        Entity actualEntry1 = Iterables.get(entries, 0);
        assertTrue(actualEntry1.equals(entry) || actualEntry1.equals(entry2) || actualEntry1.equals(entry3));

        Entity actualEntry2 = Iterables.get(entries, 0);
        assertTrue(actualEntry2.equals(entry) || actualEntry2.equals(entry2) || actualEntry2.equals(entry3));

        Entity actualEntry3 = Iterables.get(entries, 0);
        assertTrue(actualEntry3.equals(entry) || actualEntry3.equals(entry2) || actualEntry3.equals(entry3));
    }

}
