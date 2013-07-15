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
package org.calrissian.accumulorecipes.blobstore.ext.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;

/**
 * This is an implementation of the blob store using a single large batch writer to the blob store.
 * This allows for a larger amount of data to be written at once, but by using a single writer there
 * is the possibility that data from several storage streams could be intermingled during a write.  This
 * means data flushed from one stream will actually flush data for all streams that are currently opened.
 * In most instances this should not be a problem as the data will be written anyway.
 *
 */
public class HighSpeedIngestBlobStore extends ExtendedAccumuloBlobStore {

    private final BatchWriter mainWriter;

    public HighSpeedIngestBlobStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector);
        mainWriter = super.getWriter();
    }

    public HighSpeedIngestBlobStore(Connector connector, String tableName, StoreConfig config) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, tableName, config);
        mainWriter = super.getWriter();
    }

    public HighSpeedIngestBlobStore(Connector connector, int bufferSize) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, bufferSize);
        mainWriter = super.getWriter();
    }

    public HighSpeedIngestBlobStore(Connector connector, String tableName, StoreConfig config, int bufferSize) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, tableName, config, bufferSize);
        mainWriter = super.getWriter();
    }

    /**
     * Will close all underlying resources
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        mainWriter.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BatchWriter getWriter() throws TableNotFoundException {
        //Return a batch writer that ignores close calls.
        return ignoreClose(mainWriter);
    }

    /**
     * Wraps a {@link BatchWriter} and forwards all calls to the provided writer except close
     * calls.
     */
    private static BatchWriter ignoreClose(final BatchWriter writer) {
        return new BatchWriter() {
            @Override
            public void addMutation(Mutation m) throws MutationsRejectedException {
                writer.addMutation(m);
            }

            @Override
            public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
                writer.addMutations(iterable);
            }

            @Override
            public void flush() throws MutationsRejectedException {
                writer.flush();
            }

            @Override
            public void close() throws MutationsRejectedException {
                //ignore close calls;
            }
        };
    }
}
