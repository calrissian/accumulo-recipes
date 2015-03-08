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
package org.calrissian.accumulorecipes.changelog.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumlorecipes.changelog.domain.BucketHashLeaf;
import org.calrissian.accumlorecipes.changelog.impl.AccumuloChangelogStore;
import org.calrissian.accumlorecipes.changelog.support.BucketSize;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccumuloChangelogStoreTest {

    private AccumuloChangelogStore store;

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Before
    public void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        store = new AccumuloChangelogStore(getConnector());
    }

    @Test
    public void singleChange() throws Exception {


        long currentTime = currentTimeMillis();

        /**
         * First, we build an empty source tree
         */
        MerkleTree sourceTree = buildTree();

        /**
         * Put some data into the store to represent changes in the underlying data
         */
        Event entry = createStoreEntry("1", currentTime);

        store.put(asList(entry));

        /**
         * We are querying both trees for the same time period (the exact timestamp doesn't need to be exact, though it
         * needs to be within the same set of buckets (the same 5 minute period). This is easy to accomplish as the
         * system requesting the changes can send the target systems the time range for which to criteria.
         */

        MerkleTree targetTree = buildTree();

        assertEquals(sourceTree.getNumLeaves(), targetTree.getNumLeaves());        // verify same dimensionality and number of leaves.
        assertEquals(sourceTree.getDimensions(), targetTree.getDimensions());

        List<BucketHashLeaf> diffLeaves = targetTree.diff(sourceTree);

        // the we had a difference in one bucket from the source to the target. T
        assertEquals(1, diffLeaves.size());

        long expectedBucket = currentTime - (currentTime % BucketSize.FIVE_MINS.getMs());
        assertEquals(expectedBucket, diffLeaves.get(0).getTimestamp());
    }

    @Test
    public void multipleChanges() throws Exception {

        long currentTime = currentTimeMillis();

        MerkleTree sourceTree = buildTree();

        Event entry = createStoreEntry("1", currentTime);
        Event entry2 = createStoreEntry("2", currentTime - 900000);
        Event entry3 = createStoreEntry("3", currentTime - 50000000);
        Event entry4 = createStoreEntry("4", currentTime);
        Event entry5 = createStoreEntry("5", currentTime + 5000000);

        store.put(asList(entry, entry2, entry3, entry4, entry5));

        MerkleTree targetTree = buildTree();

        assertEquals(sourceTree.getNumLeaves(), targetTree.getNumLeaves());        // verify same dimensionality and number of leaves.
        assertEquals(sourceTree.getDimensions(), targetTree.getDimensions());

        List<BucketHashLeaf> diffLeaves = targetTree.diff(sourceTree);  // these will be returned in reverse order
        assertEquals(4, diffLeaves.size());

        long expectedTime1 = (currentTime + 5000000) - ((currentTime + 5000000) % BucketSize.FIVE_MINS.getMs());
        assertEquals(expectedTime1, diffLeaves.get(0).getTimestamp());

        long expectedTime2 = (currentTime) - ((currentTime) % BucketSize.FIVE_MINS.getMs());
        assertEquals(expectedTime2, diffLeaves.get(1).getTimestamp());

        long expectedTime3 = (currentTime - 900000) - ((currentTime - 900000) % BucketSize.FIVE_MINS.getMs());
        assertEquals(expectedTime3, diffLeaves.get(2).getTimestamp());

        long expectedTime4 = (currentTime - 50000000) - ((currentTime - 50000000) % BucketSize.FIVE_MINS.getMs());
        assertEquals(expectedTime4, diffLeaves.get(3).getTimestamp());
    }


    @Test
    public void singleChange_fetched() throws Exception {

        MerkleTree sourceTree = buildTree();

        long currentTime = currentTimeMillis();

        Event entry = createStoreEntry("1", currentTime);
        store.put(asList(entry));

        MerkleTree targetTree = buildTree();

        assertEquals(sourceTree.getNumLeaves(), targetTree.getNumLeaves());        // verify same dimensionality and number of leaves.
        assertEquals(sourceTree.getDimensions(), targetTree.getDimensions());

        List<BucketHashLeaf> diffLeaves = targetTree.diff(sourceTree);
        Collection<Date> dates = new ArrayList<Date>();
        for (BucketHashLeaf leaf : diffLeaves)
            dates.add(new Date(leaf.getTimestamp()));

        Iterable<Event> entries = store.getChanges(dates, Auths.EMPTY);
        assertEquals(entry, Iterables.get(entries, 0));
    }


    @Test
    public void multipleChanges_fetched() throws Exception {

        MerkleTree sourceTree = buildTree();

        long currentTime = currentTimeMillis();

        Event entry = createStoreEntry("1", currentTime);
        Event entry2 = createStoreEntry("2", currentTime - 900000);
        Event entry3 = createStoreEntry("3", currentTime - 50000000);
        Event entry4 = createStoreEntry("4", currentTime);
        Event entry5 = createStoreEntry("5", currentTime + 5000000);


        List<Event> entryList = asList(entry, entry2, entry3, entry4, entry5);
        store.put(entryList);

        MerkleTree targetTree = buildTree();

        assertEquals(sourceTree.getNumLeaves(), targetTree.getNumLeaves());        // verify same dimensionality and number of leaves.
        assertEquals(sourceTree.getDimensions(), targetTree.getDimensions());

        List<BucketHashLeaf> diffLeaves = targetTree.diff(sourceTree);
        Collection<Date> dates = new ArrayList<Date>();
        for (BucketHashLeaf leaf : diffLeaves)
            dates.add(new Date(leaf.getTimestamp()));

        System.out.println(dates);

        Iterable<Event> entries = store.getChanges(dates, Auths.EMPTY);
        assertEquals(5, Iterables.size(entries));

        for (Event actualEntry : entries) {
            assertTrue(entryList.contains(actualEntry));
        }
    }


    private MerkleTree buildTree() {
        return store.getChangeTree(
                new Date(currentTimeMillis() - 50000000),
                new Date(currentTimeMillis() + 50000000), Auths.EMPTY);
    }

    private Event createStoreEntry(String uuid, long timestamp) {
        Event entry = new BaseEvent(uuid, timestamp);
        entry.put(new Attribute("key2", "val2"));
        entry.put(new Attribute("key3", "val3"));

        return entry;
    }
}
