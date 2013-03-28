package org.calrissian.blobstore.impl;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.recipes.accumulo.blobstore.BlobStore;
import org.calrissian.recipes.accumulo.blobstore.impl.AccumuloBlobStore;
import org.calrissian.recipes.accumulo.blobstore.support.AccumuloBlobInputStream;
import org.calrissian.recipes.accumulo.blobstore.support.BlobConstants;
import org.calrissian.recipes.accumulo.blobstore.support.BlobUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class AccumuloBlobStoreTest {

    Connector connector;

    BlobStore<AccumuloBlobInputStream> service;

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {

        Instance instance = new MockInstance();
        connector = instance.getConnector("username", "pass".getBytes());

        service = new AccumuloBlobStore(connector);
    }

    @Test
    public void testPut_PersistsSingleSequenceItem() throws TableNotFoundException {

        byte[] testBlob = buildTestBlob(BlobConstants.MAX_PARTITION_BYTES );
        InputStream is = new ByteArrayInputStream(testBlob);

        String hash = service.put(is, "test", System.currentTimeMillis(), "");

        Scanner scanner = connector.createScanner(BlobConstants.DEFAULT_TABLE_NAME, new Authorizations());

        for(Map.Entry<Key,Value> entry : scanner) {

            // should only be 1 row
            assertEquals(0, Integer.parseInt(entry.getKey().getColumnQualifier().toString()));
            assertEquals("test", entry.getKey().getColumnFamily().toString());
            assertEquals(hash, entry.getKey().getRow().toString());
        }
    }


    @Test
    public void testPut_PersistsMultipleSequenceItem() throws TableNotFoundException {

        byte[] testBlob = buildTestBlob(BlobConstants.MAX_PARTITION_BYTES + 1);

        InputStream is = new ByteArrayInputStream(testBlob);

        byte[] copied = Arrays.copyOf(testBlob, testBlob.length -1 );

        String hash = service.put(is, "test", System.currentTimeMillis(), "");

        int count = 0;
        Scanner scanner = connector.createScanner(BlobConstants.DEFAULT_TABLE_NAME, new Authorizations());

        for(Map.Entry<Key,Value> entry : scanner) {

            // should be 2 rows

            if(count == 0) {
                assertEquals(0, Integer.parseInt(entry.getKey().getColumnQualifier().toString()));
            }

            else {
                assertEquals(1, Integer.parseInt(entry.getKey().getColumnQualifier().toString()));
            }

            assertEquals("test", entry.getKey().getColumnFamily().toString());
            assertEquals(hash, entry.getKey().getRow().toString());

            count++;
        }
    }

    @Test
    public void testGet_SingleSequenceItem() throws IOException, TableNotFoundException {

        byte[] testBlob = buildTestBlob(BlobConstants.MAX_PARTITION_BYTES );
        byte[] actualBytes = new byte[testBlob.length];

        InputStream is = new ByteArrayInputStream(testBlob);

        String hash = service.put(is, "test", System.currentTimeMillis(), "");

        debugTables();

        InputStream actualBlobIS = service.get(hash, "test", new Authorizations());

        actualBlobIS.read(actualBytes);

        assertEquals(BlobUtils.hashBytes(testBlob), BlobUtils.hashBytes(actualBytes));
    }

    @Test
    public void testGet_MultipleSequenceItems() throws IOException, TableNotFoundException {

        byte[] testBlob = buildTestBlob(BlobConstants.MAX_PARTITION_BYTES + 1);
        byte[] copied = Arrays.copyOf(testBlob, testBlob.length -1 );


        InputStream is = new ByteArrayInputStream(testBlob);

        String hash = service.put(is, "test", System.currentTimeMillis(), "");

        debugTables();

        InputStream actualBlobIS = service.get(hash, "test", new Authorizations());

        byte[] actualBytes = new byte[testBlob.length];

        actualBlobIS.read(actualBytes);

        assertEquals(BlobUtils.hashBytes(testBlob), BlobUtils.hashBytes(actualBytes));
    }

    @Test
    public void testGet_WithHeaders() throws TableNotFoundException, IOException {

        byte[] testBlob = buildTestBlob(BlobConstants.MAX_PARTITION_BYTES + 1);

        Map<String,String> headers = new HashMap<String,String>();
        headers.put("testKey1", "testVal1");
        headers.put("testKey2", "testVal2");


        InputStream is = new ByteArrayInputStream(testBlob);

        String hash = service.put(is, "test", System.currentTimeMillis(), "", headers);

        debugTables();

        AccumuloBlobInputStream actualBlobIS = service.get(hash, "test", new Authorizations());

        assertEquals("testVal1", actualBlobIS.getHeaders().get("testKey1"));
        assertEquals("testVal2", actualBlobIS.getHeaders().get("testKey2"));
    }

    private void debugTables() throws TableNotFoundException {

        Scanner scanner = connector.createScanner(BlobConstants.DEFAULT_TABLE_NAME, new Authorizations());

        for(Map.Entry<Key,Value> entry : scanner) {

            System.out.println("ROW: " + entry.getKey().getRow() + " : " + entry.getKey().getColumnFamily().toString() + ":" +
                    entry.getKey().getColumnQualifier().toString());
        }
    }



    private byte[] buildTestBlob(int size) {

        byte[] testBlob = new byte[size];

        for(int i = 0; i < size; i++) {
            testBlob[i] = (byte)Math.abs(i);
        }

        return testBlob;
    }

}
