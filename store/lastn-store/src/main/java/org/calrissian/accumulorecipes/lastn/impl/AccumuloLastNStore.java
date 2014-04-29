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
package org.calrissian.accumulorecipes.lastn.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.lastn.LastNStore;
import org.calrissian.accumulorecipes.lastn.iterator.EntryIterator;
import org.calrissian.accumulorecipes.lastn.iterator.IndexEntryFilteringIterator;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.json.tuple.TupleModule;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.EnumSet.allOf;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.calrissian.accumulorecipes.lastn.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.lastn.support.Constants.DELIM_END;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;

/**
 * Accumulo implementation of the LastN Store. This will try to create and configure the necessary table and properties
 * if the necessary permissions have been granted. NOTE: If the tables need to be created manually, be sure to set the
 * maxVersions property for all scopes of the versioning iterator to your N value. Also, add the IndexEntryFilteringIterator
 * at priority 40.
 */
public class AccumuloLastNStore implements LastNStore {

    private static final String DEFAULT_TABLE_NAME = "lastN";
    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000L, 10000L, 3);
    private static final IteratorSetting EVENT_FILTER_SETTING =
            new IteratorSetting(40, "eventFilter", IndexEntryFilteringIterator.class);

    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;
    private final TypeRegistry<String> typeRegistry;
    private final ObjectMapper objectMapper;

    private Function<Entry<Key, Value>, StoreEntry> storeTransform = new Function<Entry<Key, Value>, StoreEntry>() {
        @Override
        public StoreEntry apply(Entry<Key, Value> entry) {
            try {
                return objectMapper.readValue(entry.getValue().get(), StoreEntry.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    /**
     * Uses the default tableName, store config, and maxVersions
     * @param connector
     */
    public AccumuloLastNStore(Connector connector) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, 100);
    }

    /**
     * Uses the default tableName and store config
     * @param connector
     */
    public AccumuloLastNStore(Connector connector, int maxVersions) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG, maxVersions);
    }

    /**
     * Uses the specified tableName, store config, and maxVersions
     * @param connector
     */
    public AccumuloLastNStore(Connector connector, String tableName, StoreConfig config, int maxVersions) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        checkNotNull(connector);
        checkNotNull(tableName);
        checkNotNull(config);

        this.connector = connector;
        this.tableName = tableName;
        this.typeRegistry = ACCUMULO_TYPES; //TODO allow caller to pass in types.
        this.objectMapper = new ObjectMapper().registerModule(new TupleModule(typeRegistry));

        if(!connector.tableOperations().exists(this.tableName)) {
            connector.tableOperations().create(this.tableName, true);
            configureTable(connector, this.tableName, maxVersions);
        }

        this.writer = this.connector.createBatchWriter(this.tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    /**
     * Utility method to update the correct iterators to the table.
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureTable(Connector connector, String tableName, int maxVersions) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        connector.tableOperations().attachIterator(tableName, EVENT_FILTER_SETTING, allOf(IteratorScope.class));

        connector.tableOperations().setProperty(tableName, "table.iterator.majc.vers.opt.maxVersions", Integer.toString(maxVersions));
        connector.tableOperations().setProperty(tableName, "table.iterator.minc.vers.opt.maxVersions", Integer.toString(maxVersions));
        connector.tableOperations().setProperty(tableName, "table.iterator.scan.vers.opt.maxVersions", Integer.toString(maxVersions));
    }

    /**
     * Free up threads from the batch writer.
     * @throws Exception
     */
    public void shutdown() throws MutationsRejectedException {
        writer.close();
    }

    /**
     * Add the index which will be managed by the versioning iterator and the data rows to scan from the index
     * @param group
     * @param entry
     */
    @Override
    public void put(String group, StoreEntry entry) {
        checkNotNull(group);
        checkNotNull(entry);

        // first put the main index pointing to the contextId (The column family is prefixed with the DELIM to guarantee it shows up first
        Mutation indexMutation = new Mutation(group);
        indexMutation.put(DELIM + "INDEX", "", new ColumnVisibility(), entry.getTimestamp(), new Value(entry.getId().getBytes()));

        for (Tuple tuple : entry.getTuples()) {
            String fam = String.format("%s%s", DELIM_END, entry.getId());
            Object value = tuple.getValue();
            try {
                String serialize = typeRegistry.encode(value);
                String aliasForType = typeRegistry.getAlias(value);
                String qual = String.format("%s%s%s%s%s", tuple.getKey(), DELIM, serialize, DELIM, aliasForType);
                indexMutation.put(fam, qual, new ColumnVisibility(tuple.getVisibility()), entry.getTimestamp(),
                        new Value("".getBytes()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            writer.addMutation(indexMutation);
        } catch (MutationsRejectedException ex) {
            throw new RuntimeException("There was an error writing the mutation for [index=" + group + ",entryId=" + entry.getId() + "]", ex);
        }
    }

    /**
     * Pull back the last N entries. EntryIterator will group entry tuples into a single object on the server side.
     * @param index
     * @param auths
     * @return
     */
    @Override
    public Iterable<StoreEntry> get(String index, Auths auths) {
        checkNotNull(index);
        checkNotNull(auths);

        try {
            Scanner scanner = connector.createScanner(tableName, auths.getAuths());
            scanner.setRange(new Range(index));
            scanner.fetchColumnFamily(new Text(DELIM + "INDEX"));

            IteratorSetting iteratorSetting = new IteratorSetting(16, "eventIterator", EntryIterator.class);
            scanner.addScanIterator(iteratorSetting);

            return transform(scanner, storeTransform);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


