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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumlorecipes.changelog.domain.BucketHashLeaf;
import org.calrissian.accumlorecipes.changelog.impl.AccumuloChangelogStore;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AccumuloChangelogStoreTest {

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    //@Ignore //This is just broken.  Need to see what was intended for this test.
    @Test
    public void test() throws Exception {

        AccumuloChangelogStore store = new AccumuloChangelogStore(getConnector());

        MerkleTree mt = store.getChangeTree(
                new Date(currentTimeMillis() - 50000000),
                new Date(currentTimeMillis() + 50000000), new Authorizations());

        StoreEntry entry = createStoreEntry("1", currentTimeMillis());
        StoreEntry entry2 = createStoreEntry("2", currentTimeMillis() - 900000);
        StoreEntry entry3 = createStoreEntry("3", currentTimeMillis() - 50000000);
        StoreEntry entry4 = createStoreEntry("4", currentTimeMillis());
        StoreEntry entry5 = createStoreEntry("5", currentTimeMillis() + 5000000);

        store.put(asList(entry, entry2, entry3, entry4, entry5));

        MerkleTree mt2 = store.getChangeTree(
                new Date(currentTimeMillis() - 50000000),
                new Date(currentTimeMillis() + 50000000), new Authorizations());

        /**
         * Now would be the time you'd pull the merkle tree from the foreign host and diff the remote with the local
         * (in that direction) to find out which leaves on the remote host differ from the leaves in the local host.
         */

        assertEquals(mt.getNumLeaves(), mt2.getNumLeaves());
        assertEquals(mt.getDimensions(), mt2.getDimensions());

        List<BucketHashLeaf> diffLeaves = mt2.diff(mt);
        assertEquals(4, diffLeaves.size());

        /**
         * This call to "getChanges()" would be done with the result of diffing the two local merkle tree against
         * the merkle trees of foreign hosts and getting the "buckets" that differ. One the buckets that differ are
         * known, we just need to transmit the data in those buckets.
         *
         * Let's assume that the bucket with timestamp 0 was different and we want to re-transmit just that bucket
         */
        List<Date> dates = new ArrayList<Date>();
        for(BucketHashLeaf hashLeaf : diffLeaves) {
            dates.add(new Date(hashLeaf.getTimestamp()));
        }
    }

    private StoreEntry createStoreEntry(String uuid , long timestamp) {
        StoreEntry entry = new StoreEntry(uuid, timestamp);
        entry.put(new Tuple("key2", "val2", ""));
        entry.put(new Tuple("key3", "val3", ""));

        return entry;
    }
}
