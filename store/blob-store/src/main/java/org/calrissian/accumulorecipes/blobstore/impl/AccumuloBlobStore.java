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
package org.calrissian.accumulorecipes.blobstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.blobstore.BlobStore;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.mango.io.AbstractBufferedInputStream;
import org.calrissian.mango.io.AbstractBufferedOutputStream;
import org.calrissian.mango.types.TypeEncoder;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.Validate.*;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.calrissian.mango.types.LexiTypeEncoders.integerEncoder;

/**
 * An accumulo representation of the blob store. For purposes of simplicity, current implementation only stores data
 * that can be loaded into a byte array in memory- the interface uses standard Streams to allow future
 * implementations to partition large blobs over several different rows that can be streamed from Accumulo.
 * <p/>
 * Row format is as follows:
 * <p/>
 * RowId:               key\u0000type
 * Column Family:       DATA
 * Column Qualifier:    sequence#
 * Value:               byte[]
 */
public class AccumuloBlobStore implements BlobStore {

    private static final TypeEncoder<Integer, String> encoder = integerEncoder();

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final String DEFAULT_TABLE_NAME = "blobstore";
    private static final String DATA_CF = "DATA";

    protected final Connector connector;
    protected final String tableName;
    private final StoreConfig config;
    private final int bufferSize;

    public AccumuloBlobStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_BUFFER_SIZE);
    }

    public AccumuloBlobStore(Connector connector, String tableName, StoreConfig config) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, tableName, config, DEFAULT_BUFFER_SIZE);
    }

    public AccumuloBlobStore(Connector connector, int bufferSize) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_TABLE_NAME, new StoreConfig(1, bufferSize * 100, 100, 1), bufferSize);
    }

    public AccumuloBlobStore(Connector connector, String tableName, StoreConfig config, int bufferSize) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        notNull(connector, "Invalid connector");
        notEmpty(tableName, "The table name must not be empty");
        notNull(config, "Invalid Config");
        isTrue(bufferSize > 0, "The buffer size must be greater than 0");

        this.connector = connector;
        this.tableName = tableName;
        this.config = config;
        this.bufferSize = bufferSize;

        if (!connector.tableOperations().exists(tableName)) {
            connector.tableOperations().create(tableName);
            configureTable(connector, tableName);
        }
    }

    /**
     * Helper method to generate the rowID for the data mutations.
     *
     * @param key
     * @param type
     * @return
     */
    protected static String generateRowId(String key, String type) {
        return defaultString(key) + "\u0000" + defaultString(type);
    }

    /**
     * Utility method to update the correct iterators to the table.
     *
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Nothing to do for default implementation
    }

    /**
     * Returns a new batch writer for the table.
     *
     * @return
     * @throws TableNotFoundException
     */
    protected BatchWriter getWriter() throws TableNotFoundException {
        return this.connector.createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    /**
     * Helper method to generate a mutation for each chunk of data that is being stored.
     *
     * @param key
     * @param type
     * @param data
     * @param sequenceNum
     * @param timestamp
     * @param visibility
     * @return
     */
    protected Mutation generateMutation(String key, String type, byte[] data, int sequenceNum, long timestamp, ColumnVisibility visibility) {

        Mutation mutation = new Mutation(generateRowId(key, type));
        mutation.put(DATA_CF, encoder.encode(sequenceNum), visibility, timestamp, new Value(data));

        return mutation;
    }

    /**
     * Helper method to generate an {@link OutputStream} for storing data into Accumulo.
     *
     * @param writer
     * @param key
     * @param type
     * @param timestamp
     * @param visibility
     * @return
     */
    protected OutputStream generateWriteStream(final BatchWriter writer, final String key, final String type, final long timestamp, String visibility) {

        final ColumnVisibility colVis = new ColumnVisibility(defaultString(visibility));

        return new AbstractBufferedOutputStream(bufferSize) {
            int sequenceNum = 0;

            @Override
            protected void writeBuffer(byte[] buf) throws IOException {
                if (buf.length == 0)
                    return;
                sequenceNum++;
                try {
                    writer.addMutation(generateMutation(key, type, buf, sequenceNum, timestamp, colVis));
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            @Override
            public void flush() throws IOException {
                super.flush();
                try {
                    writer.flush();
                } catch (MutationsRejectedException e) {
                    throw new IOException(e);
                }
            }

            @Override
            public void close() throws IOException {
                super.close();
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream store(String key, String type, long timestamp, String visibility) {
        try {

            return generateWriteStream(getWriter(), key, type, timestamp, visibility);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream get(String key, String type, Auths auths) {
        notNull(auths, "Null authorizations");

        try {
            String rowId = generateRowId(key, type);
            //Scan for a range including only the data
            Scanner scanner = connector.createScanner(tableName, auths.getAuths());
            scanner.setRange(Range.exact(rowId, DATA_CF));
            scanner.fetchColumnFamily(new Text(DATA_CF));

            final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

            //Create an input stream that will read the values from the iterator.
            return new AbstractBufferedInputStream() {
                @Override
                protected boolean isEOF() {
                    return !iterator.hasNext();
                }

                @Override
                protected byte[] getNextBuffer() throws IOException {
                    if (iterator.hasNext())
                        return iterator.next().getValue().get();

                    return null;
                }
            };
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder {
        private final Connector connector;
        private String tableName = DEFAULT_TABLE_NAME;
        private StoreConfig config;
        private int bufferSize = DEFAULT_BUFFER_SIZE;

        public Builder(Connector connector) {
            checkNotNull(connector);
            this.connector = connector;
        }

        public void setTableName(String tableName) {
            checkNotNull(tableName);
            this.tableName = tableName;
        }

        public void setConfig(StoreConfig config) {
            checkNotNull(config);
            this.config = config;
        }

        public void setBufferSize(int bufferSize) {
            isTrue(bufferSize > 0);
            this.bufferSize = bufferSize;
        }

        public AccumuloBlobStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            if (config==null) {
                config = new StoreConfig(1, bufferSize * 100, 100, 1);
            }
            return new AccumuloBlobStore(connector, tableName, config, bufferSize);
        }
    }

}
