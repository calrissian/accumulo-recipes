package org.calrissian.accumulorecipes.changelog.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumlorecipes.changelog.impl.AccumuloChangelogStore;
import org.calrissian.accumlorecipes.changelog.iterator.BucketHashIterator;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class AccumuloChangelogStoreTest {

    Connector connector;
    AccumuloChangelogStore store;

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {

        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "password".getBytes());

        store = new AccumuloChangelogStore(connector);
    }

    @Test
    public void test() throws TableNotFoundException {

        StoreEntry entry = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        entry.put(new Tuple("key1", "val1", ""));
        entry.put(new Tuple("key2", "val2", ""));

        StoreEntry entry2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() + 50);
        entry2.put(new Tuple("key1", "val1", ""));
        entry2.put(new Tuple("key2", "val2", ""));


        StoreEntry entry3 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000000);
        entry3.put(new Tuple("key2", "val2", ""));
        entry3.put(new Tuple("key3", "val3", ""));

        store.put(Arrays.asList(new StoreEntry[] { entry, entry2, entry3 }));

        printTable();
    }

    protected void printTable() throws TableNotFoundException {

        Scanner scanner = connector.createScanner(store.getTableName(), new Authorizations());

        IteratorSetting is = new IteratorSetting(2, BucketHashIterator.class);
        scanner.addScanIterator(is);

        for(Map.Entry<Key,Value> entry : scanner) {
            System.out.println(entry);
        }
    }

}
