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
package org.calrissian.accumulorecipes.blobstore.impl.ext;


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.blobstore.ext.impl.ExtendedAccumuloBlobStore;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class ExtendedAccumuloBlobStoreTest {

    private static final int CHUNK_SIZE = 16; //small chunk size for testing
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Collection<String>> strColRef = new TypeReference<Collection<String>>() {};

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Test
    public void testSaveAndQuerySingleChunkStream() throws Exception {
        byte[] testBlob = buildTestBlob(CHUNK_SIZE);
        ExtendedAccumuloBlobStore blobStore = new ExtendedAccumuloBlobStore(getConnector(), CHUNK_SIZE);


        OutputStream storageStream = blobStore.store("test", "1", currentTimeMillis(), "");
        storageStream.write(testBlob);
        storageStream.close();

        assertEquals(CHUNK_SIZE, blobStore.blobSize("test", "1", new Auths()));

        byte[] actual = new byte[testBlob.length];
        InputStream retrievalStream = blobStore.get("test", "1", new Auths());
        retrievalStream.read(actual);
        retrievalStream.close();

        assertArrayEquals(testBlob, actual);
    }

    @Test
    public void testSaveAndQueryMultiChunkStream() throws Exception {
        byte[] testBlob = buildTestBlob(CHUNK_SIZE * 2);
        ExtendedAccumuloBlobStore blobStore = new ExtendedAccumuloBlobStore(getConnector(), CHUNK_SIZE);


        OutputStream storageStream = blobStore.store("test", "1", currentTimeMillis(), "");
        storageStream.write(testBlob);
        storageStream.close();

        assertEquals(CHUNK_SIZE * 2, blobStore.blobSize("test", "1", new Auths()));

        byte[] actual = new byte[testBlob.length];
        InputStream retrievalStream = blobStore.get("test", "1", new Auths());
        retrievalStream.read(actual);
        retrievalStream.close();

        assertArrayEquals(testBlob, actual);
    }

    @Test
    public void testSaveAndQueryComplex() throws Exception {
        ExtendedAccumuloBlobStore blobStore = new ExtendedAccumuloBlobStore(getConnector(), CHUNK_SIZE);

        Collection<String> testValues = new ArrayList<String>(10);
        for (int i = 0; i < CHUNK_SIZE; i++)
            testValues.add(randomUUID().toString());

        //Store json in a Gzipped format
        OutputStream storageStream = blobStore.store("test", "1", currentTimeMillis(), "");
        mapper.writeValue(new GZIPOutputStream(storageStream), testValues);

        //reassemble the json after unzipping the stream.
        InputStream retrievalStream = blobStore.get("test", "1", new Auths());
        Collection<String> actualValues = mapper.readValue(new GZIPInputStream(retrievalStream), strColRef);

        //if there were no errors, then verify that the two collections are equal.
        assertThat(actualValues, is(equalTo(testValues)));
    }

    @Test
    public void testSaveAndQueryMultiChunkStreamWithProps() throws Exception {
        byte[] testBlob = buildTestBlob(CHUNK_SIZE * 2);
        ExtendedAccumuloBlobStore blobStore = new ExtendedAccumuloBlobStore(getConnector(), CHUNK_SIZE);

        Map<String, String> testProps = new LinkedHashMap<String, String>(2);
        testProps.put("value1", "one");
        testProps.put("testing", "blah");

        OutputStream storageStream = blobStore.store("test", "1", testProps, currentTimeMillis(), "");
        storageStream.write(testBlob);
        storageStream.close();

        assertEquals(CHUNK_SIZE * 2, blobStore.blobSize("test", "1", new Auths()));

        byte[] actual = new byte[testBlob.length];
        InputStream retrievalStream = blobStore.get("test", "1", new Auths());
        retrievalStream.read(actual);
        retrievalStream.close();

        assertArrayEquals(testBlob, actual);

        //now test props
        Map<String, String> retrievedProps = blobStore.getProperties("test", "1", new Auths());
        assertEquals(testProps.size() ,retrievedProps.size());

        assertThat(retrievedProps, is(equalTo(testProps)));

    }

    private byte[] buildTestBlob(int size) {

        byte[] testBlob = new byte[size];

        for(int i = 0; i < size; i++) {
            testBlob[i] = (byte)Math.abs(i);
        }

        return testBlob;
    }
}
