package org.calrissian.accumulorecipes.geospatialstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Collections;
import java.util.Map;

public class AccumuloGeoSpatialStoreTest {

    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());

        AccumuloGeoSpatialStore store = new AccumuloGeoSpatialStore(connector);

        StoreEntry entry = new StoreEntry();
        entry.put(new Tuple("Key1", "Val1", ""));
        entry.put(new Tuple("key2", "val2", ""));

        store.put(Collections.singleton(entry), new Point2D.Double(0, 0));


        store.get(new Rectangle2D.Double(-1.0, -1.0, 2.0 , 2.0), new Auths());
        Scanner scanner = connector.createScanner("geoStore", new Authorizations());
        for(Map.Entry<Key,Value> curEntry : scanner) {
            System.out.println(curEntry);
        }
    }

}
