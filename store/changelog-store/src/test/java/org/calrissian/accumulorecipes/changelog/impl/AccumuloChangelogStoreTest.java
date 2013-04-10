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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

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
    public void test() throws TableNotFoundException, IOException {

        System.out.println("CURRENT TIME: " + new Date(System.currentTimeMillis()));
        StoreEntry entry = createStoreEntry("1", System.currentTimeMillis());
        StoreEntry entry2 = createStoreEntry("2", 0);
        StoreEntry entry3 = createStoreEntry("3", 50000000);
        StoreEntry entry4 = createStoreEntry("4", 50000000);
        StoreEntry entry5 = createStoreEntry("5", 500000);

        store.put(Arrays.asList(new StoreEntry[] { entry, entry2, entry3, entry4, entry5 }));

        System.out.println(
                ObjectMapperContext.getInstance().getObjectMapper().writeValueAsString(
                        store.getChangeTree(new Date(0), new Date(System.currentTimeMillis()))));
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
