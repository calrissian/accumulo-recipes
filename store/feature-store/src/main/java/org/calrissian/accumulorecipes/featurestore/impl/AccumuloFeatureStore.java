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
package org.calrissian.accumulorecipes.featurestore.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.util.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.util.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry.BASE_FEATURES;
import static org.calrissian.accumulorecipes.featurestore.support.Utilities.combine;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.FeatureStore;
import org.calrissian.accumulorecipes.featurestore.model.Feature;
import org.calrissian.accumulorecipes.featurestore.support.Constants;
import org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry;
import org.calrissian.accumulorecipes.featurestore.support.FeatureTransform;
import org.calrissian.accumulorecipes.featurestore.support.GroupIndexIterator;
import org.calrissian.accumulorecipes.featurestore.support.TypeIndexIterator;
import org.calrissian.accumulorecipes.featurestore.support.config.AccumuloFeatureConfig;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Pair;

/**
 * This class will store simple feature vectors into accumulo.  The features will aggregate over predefined time intervals
 * and the aggregations are available immediately.
 * <p/>
 * Format of the table:
 * Rowid                CF                       CQ                  Value
 * group\u0000revTS     model\u0000'MINUTES'     type\u0000name      value
 * group\u0000revTS     model\u0000'HOURS'       type\u0000name      value
 * group\u0000revTS     model\u0000'DAYS'        type\u0000name      value
 * group\u0000revTS     model\u0000'MONTHS'      type\u0000name      value
 * <p/>
 */
public class AccumuloFeatureStore implements FeatureStore {

    public static final String DEFAULT_TABLE_NAME = "features";
    public static final String REVERSE_SUFFIX = "_reverse";
    public static final String INDEX_SUFFIX = "_index";
    protected static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000, 100, 10);

    private final Connector connector;
    private final StoreConfig config;
    private final String tableName;
    private BatchWriter groupWriter;
    private BatchWriter typeWriter;
    private BatchWriter indexWriter;

    private boolean isInitialized = false;

    protected final FeatureRegistry registry;

    public AccumuloFeatureStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG, BASE_FEATURES);
    }

    public AccumuloFeatureStore(Connector connector, String tableName, StoreConfig config, FeatureRegistry featureRegistry) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        checkNotNull(connector);
        checkNotNull(tableName);
        checkNotNull(config);

        this.connector = connector;
        this.tableName = tableName;
        this.config = config;
        this.registry = featureRegistry;

    }

    public void initialize() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        createTable(this.tableName);
        createTable(this.tableName + REVERSE_SUFFIX);

        if(!connector.tableOperations().exists(tableName + INDEX_SUFFIX))
            connector.tableOperations().create(tableName + INDEX_SUFFIX, true);

        this.groupWriter = this.connector.createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
        this.typeWriter = this.connector.createBatchWriter(tableName + REVERSE_SUFFIX, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
        this.indexWriter = this.connector.createBatchWriter(tableName + INDEX_SUFFIX, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());

        isInitialized = true;
    }

    private void createTable(String tableName) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        int priority = DEFAULT_ITERATOR_PRIORITY;
        if (!connector.tableOperations().exists(tableName)) {
            connector.tableOperations().create(tableName, false);
            for (AccumuloFeatureConfig featureConfig : registry.getConfigs()) {
                List<IteratorSetting> settings = featureConfig.buildIterators(priority);

                int numSettings = 0;
                for (IteratorSetting setting : settings) {
                    numSettings++;
                    connector.tableOperations().attachIterator(tableName, setting);
                }

                priority += numSettings;
            }
        }
    }


    protected ScannerBase metricScanner(AccumuloFeatureConfig xform, Date start, Date end, String group, Iterable<String> types, String name, TimeUnit timeUnit, Auths auths) {
        checkNotNull(xform);

        try {
            group = defaultString(group);
            timeUnit = (timeUnit == null ? TimeUnit.MINUTES : timeUnit);

            BatchScanner scanner = connector.createBatchScanner(tableName + REVERSE_SUFFIX, auths.getAuths(), config.getMaxQueryThreads());

            Collection<Range> typeRanges = new ArrayList();
            for (String type : types)
                typeRanges.add(buildRange(type, start, end, timeUnit));

            scanner.setRanges(typeRanges);
            scanner.fetchColumn(new Text(combine(timeUnit.toString(), xform.featureName())), new Text(combine(group, name)));

            return scanner;

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected ScannerBase metricScanner(AccumuloFeatureConfig xform, Date start, Date end, String group, String type, String name, TimeUnit timeUnit, Auths auths) {
        checkNotNull(xform);

        try {

            group = defaultString(group);
            timeUnit = (timeUnit == null ? TimeUnit.MINUTES : timeUnit);

            Scanner scanner = connector.createScanner(tableName + REVERSE_SUFFIX, auths.getAuths());
            scanner.setRange(buildRange(type, start, end, timeUnit));

            if (group != null && name != null) {
                scanner.fetchColumn(new Text(combine(timeUnit.toString(), xform.featureName())), new Text(combine(group, name)));
            } else {
                scanner.fetchColumnFamily(new Text(combine(timeUnit.toString(), xform.featureName())));

                String cqRegex = null;
                if (group != null) {
                    cqRegex = combine(group, "(.*)");
                } else if (name != null)
                    cqRegex = combine("(.*)", name);
                if (cqRegex != null) {
                    IteratorSetting regexIterator = new IteratorSetting(Constants.DEFAULT_ITERATOR_PRIORITY - 1, "regex", RegExFilter.class);
                    scanner.addScanIterator(regexIterator);
                }
            }

            return scanner;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected Range buildRange(String type, Date start, Date end, TimeUnit timeUnit) {
        return new Range(
                combine(type, generateTimestamp(end.getTime(), timeUnit)),
                combine(type, generateTimestamp(start.getTime(), timeUnit))
        );
    }

    /**
     * Will close all underlying resources
     *
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        groupWriter.close();
        typeWriter.close();
    }


    public void save(Iterable<? extends Feature> featureData) {
        save(featureData, asList(TimeUnit.values()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(Iterable<? extends Feature> featureData, Iterable<TimeUnit> timeUnits) {

        if (!isInitialized)
            throw new RuntimeException("Please called initialize() on the store first");

        Set<Pair<String, String>> indices = new HashSet<Pair<String,String>>();

        try {
            for (Feature feature : featureData) {

                AccumuloFeatureConfig featureConfig = registry.transformForClass(feature.getClass());

                if (featureConfig == null)
                    throw new RuntimeException("Skipping unknown model type: " + feature.getClass());

                //fix null values
                String group = defaultString(feature.getGroup());
                String type = defaultString(feature.getType());
                String name = defaultString(feature.getName());
                ColumnVisibility visibility = new ColumnVisibility(defaultString(feature.getVisibility()));

                for (TimeUnit timeUnit : timeUnits) {

                    String timestamp = generateTimestamp(feature.getTimestamp(), timeUnit);
                    //Create mutation with:
                    //rowID: group\u0000timestamp
                    Mutation group_mutation = new Mutation(
                            combine(group, timestamp)
                    );

                    indices.add(new Pair<String,String>(group + NULL_BYTE + type + NULL_BYTE + name, feature.getVisibility()));

                    //Create mutation with:
                    //rowID: type\u0000timestamp
                    Mutation type_mutation = new Mutation(
                            combine(type, timestamp)
                    );


                    //CF: Timeunit
                    //CQ: type\u0000name
                    group_mutation.put(
                            combine(timeUnit.toString(), featureConfig.featureName()),
                            combine(type, name),
                            visibility,
                            feature.getTimestamp(),
                            featureConfig.buildValue(feature)
                    );

                    //CF: Timeunit
                    //CQ: group\u0000name
                    type_mutation.put(
                            combine(timeUnit.toString(), featureConfig.featureName()),
                            combine(group, name),
                            visibility,
                            feature.getTimestamp(),
                            featureConfig.buildValue(feature)
                    );

                    groupWriter.addMutation(group_mutation);
                    typeWriter.addMutation(type_mutation);
                }
            }

            for(Pair<String,String> entries : indices) {
                Mutation m = new Mutation(entries.getOne());
                m.put("", "", new ColumnVisibility(entries.getTwo()), EMPTY_VALUE);
                indexWriter.addMutation(m);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws Exception {
        groupWriter.flush();
        typeWriter.flush();
        indexWriter.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends Feature> CloseableIterable<T> query(Date start, Date end, String group, String type, String name, TimeUnit timeUnit, Class<T> featureType, Auths auths) {

        if (!isInitialized)
            throw new RuntimeException("Please call initialize() on the store first.");

        final AccumuloFeatureConfig<T> featureConfig = registry.transformForClass(featureType);

        return (CloseableIterable<T>) transform(
                closeableIterable(metricScanner(featureConfig, start, end, group, type, name, timeUnit, auths)),
                buildFeatureTransform(featureConfig)
        );
    }

    @Override
    public <T extends Feature> CloseableIterable<T> query(Date start, Date end, String group, Set<String> types, String name, TimeUnit timeUnit, Class<T> featureType, Auths auths) {

        if (!isInitialized)
            throw new RuntimeException("Please call initialize() on the store first.");

        final AccumuloFeatureConfig<T> featureConfig = registry.transformForClass(featureType);

        return (CloseableIterable<T>) transform(
            closeableIterable(metricScanner(featureConfig, start, end, group, types, name, timeUnit, auths)),
            buildFeatureTransform(featureConfig)
        );
    }

    @Override
    public Iterable<String> groups(String prefix, Auths auths) {

        checkNotNull(prefix);
        checkNotNull(auths);
        try {
            BatchScanner scanner = connector.createBatchScanner(tableName + INDEX_SUFFIX, auths.getAuths(), config.getMaxQueryThreads());
            scanner.setRanges(singleton(Range.prefix(prefix)));

            IteratorSetting setting = new IteratorSetting(25, GroupIndexIterator.class);
            scanner.addScanIterator(setting);

            return transform(wrap(scanner), groupIndexTransform);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> types(String group, String prefix, Auths auths) {

        checkNotNull(group);
        checkNotNull(prefix);
        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(tableName + INDEX_SUFFIX, auths.getAuths(), config.getMaxQueryThreads());
            scanner.setRanges(singleton(Range.prefix(group + NULL_BYTE + prefix)));

            IteratorSetting setting = new IteratorSetting(25, TypeIndexIterator.class);
            scanner.addScanIterator(setting);

            return transform(wrap(scanner), typeIndexTransform);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> names(String group, String type, String prefix, Auths auths) {

        checkNotNull(group);
        checkNotNull(type);
        checkNotNull(prefix);
        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(tableName + INDEX_SUFFIX, auths.getAuths(), config.getMaxQueryThreads());
            scanner.setRanges(singleton(Range.prefix(group + NULL_BYTE + type + NULL_BYTE + prefix)));

            IteratorSetting setting = new IteratorSetting(25, FirstEntryInRowIterator.class);
            scanner.addScanIterator(setting);

            return transform(wrap(scanner), groupTypeNameIndexTransform);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected <T extends Feature>FeatureTransform<T> buildFeatureTransform(final AccumuloFeatureConfig xform) {
        return new FeatureTransform<T>() {
            @Override
            protected T transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                return (T) xform.buildFeatureFromValue(timestamp, group, type, name, visibility, value);
            }
        };
    }

    protected Function<Map.Entry<Key,Value>, String> groupIndexTransform = new Function<Map.Entry<Key,Value>,String>() {
        @Override
        public String apply(Map.Entry<Key,Value> s) {
            String row = s.getKey().getRow().toString();
            int idx = row.indexOf(NULL_BYTE);
            return row.substring(0, idx);
        }
    };

    protected Function<Map.Entry<Key,Value>, String> typeIndexTransform = new Function<Map.Entry<Key,Value>,String>() {
        @Override
        public String apply(Map.Entry<Key,Value> s) {
            String row = s.getKey().getRow().toString();
            int idx = row.indexOf(NULL_BYTE);
            int nameIdx = row.lastIndexOf(NULL_BYTE);
            return row.substring(idx+1, nameIdx);
        }
    };

    protected Function<Map.Entry<Key,Value>, String> groupTypeNameIndexTransform = new Function<Map.Entry<Key,Value>,String>() {
        @Override
        public String apply(Map.Entry<Key,Value> s) {
            String row = s.getKey().getRow().toString();
            int nameIdx = row.lastIndexOf(NULL_BYTE);
            return row.substring(nameIdx+1, row.length());
        }
    };

    public class Builder {
        private final Connector connector;
        private String tableName = DEFAULT_TABLE_NAME;
        private StoreConfig config = DEFAULT_STORE_CONFIG;
        private FeatureRegistry featureRegistry = BASE_FEATURES;

        public Builder(Connector connector) {
            checkNotNull(connector);
            this.connector = connector;
        }

        public Builder setTableName(String tableName) {
            checkNotNull(tableName);
            this.tableName = tableName;
            return this;
        }

        public Builder setConfig(StoreConfig config) {
            checkNotNull(config);
            this.config = config;
            return this;
        }

        public Builder setFeatureRegistry(FeatureRegistry featureRegistry) {
            checkNotNull(featureRegistry);
            this.featureRegistry = featureRegistry;
            return this;
        }

        public AccumuloFeatureStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            return new AccumuloFeatureStore(connector,tableName,config,featureRegistry);
        }
    }

}
