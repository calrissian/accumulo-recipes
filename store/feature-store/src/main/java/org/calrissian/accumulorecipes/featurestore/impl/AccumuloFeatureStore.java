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


import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
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
import org.calrissian.accumulorecipes.featurestore.support.config.AccumuloFeatureConfig;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry.BASE_FEATURES;
import static org.calrissian.accumulorecipes.featurestore.support.Utilities.combine;
import static org.calrissian.mango.collect.CloseableIterables.transform;

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
    protected static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000, 100, 10);

    private final Connector connector;
    private final StoreConfig config;
    private final String tableName;
    private BatchWriter groupWriter;
    private BatchWriter typeWriter;

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

        this.groupWriter = this.connector.createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
        this.typeWriter = this.connector.createBatchWriter(tableName + REVERSE_SUFFIX, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());

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
        Preconditions.checkNotNull(xform);

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
        Preconditions.checkNotNull(xform);

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

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws Exception {
        groupWriter.flush();
        typeWriter.flush();
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

    protected <T extends Feature>FeatureTransform<T> buildFeatureTransform(final AccumuloFeatureConfig xform) {
        return new FeatureTransform<T>() {
            @Override
            protected T transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                return (T) xform.buildFeatureFromValue(timestamp, group, type, name, visibility, value);
            }
        };
    }
}
