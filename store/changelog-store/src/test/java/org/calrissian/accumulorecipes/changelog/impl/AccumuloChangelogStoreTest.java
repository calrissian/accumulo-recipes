package org.calrissian.accumulorecipes.changelog.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumlorecipes.changelog.impl.AccumuloChangelogStore;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class AccumuloChangelogStoreTest {

    Connector connector;
    AccumuloChangelogStore store;

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {

        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "password".getBytes());

        store = new AccumuloChangelogStore(connector);
    }

    @Test
    public void test() throws TableNotFoundException, IOException {

        StoreEntry entry = createStoreEntry("1", System.currentTimeMillis());
        StoreEntry entry2 = createStoreEntry("2", System.currentTimeMillis() - 900000);
        StoreEntry entry3 = createStoreEntry("3", System.currentTimeMillis() - 50000000);
        StoreEntry entry4 = createStoreEntry("4", System.currentTimeMillis());
        StoreEntry entry5 = createStoreEntry("5", System.currentTimeMillis() - 900000);

        store.put(Arrays.asList(new StoreEntry[] { entry, entry2, entry3, entry4, entry5 }));

        printTable();

        MerkleTree mt = store.getChangeTree(new Date(System.currentTimeMillis() - (store.getBucketSize().getMs() * 8)), new Date(System.currentTimeMillis()));

        /**
         * Now would be the time you'd pull the merkle tree from the foreign host and diff the remote with the local
         * (in that direction) to find out which leaves on the remote host differ from the leaves in the local host.
         */
        System.out.println("MERKLE: " + mt);

        /**
         * This call to "getChanges()" would be done with the result of diffing the two local merkle tree against
         * the merkle trees of foreign hosts and getting the "buckets" that differ. One the buckets that differ are
         * known, we just need to transmit the data in those buckets.
         *
         * Let's assume that the bucket with timestamp 0 was different and we want to re-transmit just that bucket
         */
        for(StoreEntry actualEntry : store.getChanges(Collections.singleton(new Date(System.currentTimeMillis())))) {
            System.out.println(actualEntry);
        }
    }

    protected void printTable() throws TableNotFoundException, IOException {

        Scanner scanner = connector.createScanner(store.getTableName(), new Authorizations());

        for(Map.Entry<Key,Value> entry : scanner) {

            System.out.println(entry);
        }
    }

    private StoreEntry createStoreEntry(String uuid , long timestamp) {
        StoreEntry entry = new StoreEntry(uuid, timestamp);
        entry.put(new Tuple("key2", "val2", ""));
        entry.put(new Tuple("key3", "val3", ""));

        return entry;
    }
}
