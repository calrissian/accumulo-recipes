/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.blobstore.impl;

import org.calrissian.mango.types.TypeNormalizer;
import org.calrissian.mango.types.exception.TypeNormalizationException;
import org.calrissian.mango.types.normalizers.IntegerNormalizer;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.calrissian.accumulorecipes.blobstore.BlobStore;
import org.calrissian.accumulorecipes.blobstore.support.AccumuloBlobInputStream;
import org.calrissian.accumulorecipes.blobstore.support.BlobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * An accumulo representation of the blob store. For purposes of simplicity, current implementation only stores data
 * that can be loaded into a byte array in memory- the interface has been coded to InputStream to allow future
 * implementations to partition large blobs over several different rows that can be streamed from Accumulo.
 *
 * Row format is as follows:
 *
 * RowId:               md5 hash of blob
 * Column Family:       sequence #
 * Column Qualifier:    dataType
 * Value:               byte[]
 *
 */
public class AccumuloBlobStore implements BlobStore<AccumuloBlobInputStream> {

    public static Logger logger = LoggerFactory.getLogger(AccumuloBlobStore.class);

    private String tableName = BlobConstants.DEFAULT_TABLE_NAME;

    private Connector connector;
    private BatchWriter writer;

    public AccumuloBlobStore(Connector connector) {

        this.connector = connector;

        if(!connector.tableOperations().exists(tableName)) {
            createTable();
        }

        try {
            this.writer = connector.createBatchWriter(tableName, 1000000L, 100L, 10);
        } catch (TableNotFoundException e) {
            logger.error("There was error creating batch writer for " + tableName);
        }
    }

    private void createTable() {

        try {
            connector.tableOperations().create(tableName);
        } catch (Exception e) {
            logger.error("There was an error creating table " + tableName + ". exception= " + e.getMessage());
        }
    }

    /**
     * Places a blob in the blob store.
     * @param blob an input stream containing the bytes to place in the store
     * @param dataType what do the bytes represent? (i.e. image-jpeg, image-png, pcap, etc...)
     */
    @Override
    public String put(final InputStream blob, String dataType, long timestamp, String visibility) {

        return put(blob, dataType, timestamp, visibility, null);
    }

    @Override
    public String put(InputStream blob, String type, long timestamp, String visibility, Map<String, String> headers) {

        String hash = UUID.randomUUID().toString();

        put(hash, blob, type, timestamp, visibility, headers);

        return hash;
    }

    /**
     * This method can be dangerous as Accumulo creates it's splits and maps tablet based on the "key"- thus this value
     * should be as random as possible to promote parallel queries.
     * @param key
     * @param blob
     * @param type
     * @param timestamp
     * @param visibility
     */
    @Override
    public void put(String key, InputStream blob, String type, long timestamp, String visibility) {

        put(key, blob, type, timestamp, visibility, null);

    }

    @Override
    public void put(String key, InputStream blob, String type, long timestamp, String visibility, Map<String, String> headers) {

        byte[] bytes = new byte[BlobConstants.MAX_PARTITION_BYTES];
        int sequenceNumber = 0;

        try {

            if(headers != null) {

                Mutation headerMutation = new Mutation(key);

                for(Map.Entry<String,String> entry : headers.entrySet()) {

                    headerMutation.put(type, "\u0000" + entry.getKey() + "\u0000" + entry.getValue(),
                            new ColumnVisibility(visibility), timestamp, new Value("".getBytes()));
                }

                writer.addMutation(headerMutation);
            }

            Mutation m = null;

            int numbytes;
            while((numbytes = blob.read(bytes)) > 0) {

                m = new Mutation(key);

                String sequence = new IntegerNormalizer().normalize(sequenceNumber);

                m.put(type, sequence, new ColumnVisibility(visibility), timestamp, new Value(Arrays.copyOfRange(bytes, 0, numbytes)));

                sequenceNumber++;
                writer.addMutation(m);
            }

            writer.flush();

        } catch (IOException e) {
            logger.error("There was an exception reading from input stream: " + e.getMessage());
        } catch (TypeNormalizationException e) {
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            logger.error("There was an exception trying to add the sequence number: " + sequenceNumber);
        }    }

    /**
     * Retrieves a blob from the blob store
     * @param key
     * @param dataType
     * @return
     */
    @Override
    public AccumuloBlobInputStream get(String key, String dataType, Authorizations auths) {

        try {
            Scanner scanner = connector.createScanner(tableName, auths);

            TypeNormalizer normalizer = new IntegerNormalizer();

            Range range = new Range(
                    new Key(key, dataType, "\u0000"),
                    new Key(key, dataType, normalizer.normalize(Integer.MAX_VALUE))
            );

            scanner.setRange(range);
            final Iterator iterator = scanner.iterator();

            if(iterator.hasNext()) {
                return new AccumuloBlobInputStream(iterator);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void shutdown() {

        try {
            writer.close();
        } catch (MutationsRejectedException e) {
            logger.error("An error occurred closing the batch writer");
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
