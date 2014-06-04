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


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.featurestore.MetricStore;
import org.calrissian.accumulorecipes.featurestore.feature.BaseFeature;
import org.calrissian.accumulorecipes.featurestore.support.FeatureEntryTransform;
import org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry;
import org.calrissian.accumulorecipes.featurestore.feature.transform.AccumuloFeatureConfig;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry.BASE_FEATURES;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DELIM;
import static org.calrissian.mango.collect.CloseableIterables.transform;

/**
 * This class will store simple metric data into accumulo.  The metrics will aggregate over predefined time intervals
 * but are available immediately to criteria.
 * <p/>
 * Format of the table:
 * Rowid                CF                       CQ                  Value
 * group\u0000revTS     feature\u0000'MINUTES'   type\u0000name      value
 * group\u0000revTS     feature\u0000'HOURS'     type\u0000name      value
 * group\u0000revTS     feature\u0000'DAYS'      type\u0000name      value
 * group\u0000revTS     feature\u0000'MONTHS'    type\u0000name      value
 * <p/>
 * The table is configured to use a SummingCombiner against each of the columns specified.
 */
public class AccumuloFeatureStore implements MetricStore {

    public static final String DEFAULT_TABLE_NAME = "metrics";
    public static final String REVERSE_SUFFIX = "_reverse";
    protected static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000, 100, 10);

    private final Connector connector;
    private final StoreConfig config;
    private final String tableName;
    private final BatchWriter groupWriter;
    private final BatchWriter typeWriter;

    protected FeatureRegistry registry = BASE_FEATURES;

    public AccumuloFeatureStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public void setFeatureRegistry(FeatureRegistry registry) {
        this.registry = registry;
    }

    public AccumuloFeatureStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        checkNotNull(connector);
        checkNotNull(tableName);
        checkNotNull(config);

        this.connector = connector;
        this.tableName = tableName;
        this.config = config;

        createTable(this.tableName);
        createTable(this.tableName + REVERSE_SUFFIX);

        this.groupWriter = this.connector.createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
        this.typeWriter = this.connector.createBatchWriter(tableName + REVERSE_SUFFIX, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    public static String combine(String... items) {
        if (items == null)
            return null;
        return join(items, DELIM);
    }

    private void createTable(String tableName) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (!connector.tableOperations().exists(tableName)) {
            connector.tableOperations().create(tableName, false);
            for(AccumuloFeatureConfig featureConfig : registry.getConfigs()) {
                List<IteratorSetting> settings = featureConfig.buildIterators();
                for(IteratorSetting setting : settings)
                    connector.tableOperations().attachIterator(tableName, setting);
            }
        }
    }

    protected ScannerBase metricScanner(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, AccumuloFeatureConfig featureConfig, Auths auths) {
        checkNotNull(group);
        checkNotNull(type);
        checkNotNull(start);
        checkNotNull(end);
        checkNotNull(auths);

        try {
            //fix null values
            group = defaultString(group);
            timeUnit = (timeUnit == null ? MetricTimeUnit.MINUTES : timeUnit);

            //Start scanner over the known range group_end to group_start.  The order is reversed due to the use of a reverse
            //timestamp.  Which is used to provide the latest results first.
            BatchScanner scanner = connector.createBatchScanner(tableName + REVERSE_SUFFIX, auths.getAuths(), config.getMaxQueryThreads());
            scanner.setRanges(singleton(new Range(
                    combine(type, generateTimestamp(end.getTime(), timeUnit)),
                    combine(type, generateTimestamp(start.getTime(), timeUnit))
            )));

            //If both type and name are here then simply fetch the column
            //else fetch the timeunit column family and apply a regex filter for the CQ containing the type and name.
            if (name != null) {
                scanner.fetchColumn(new Text(combine(featureConfig.featureName(), timeUnit.toString())), new Text(combine(group, name)));
            } else {
                scanner.fetchColumnFamily(new Text(combine(featureConfig.featureName(), timeUnit.toString())));

                //generates the correct regex
                String cqRegex = null;
                cqRegex = combine(group, "(.*)");
                IteratorSetting regexIterator = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY - 1, "regex", RegExFilter.class);
                RegExFilter.setRegexs(regexIterator, null, null, cqRegex, null, false);
                scanner.addScanIterator(regexIterator);
            }

            return scanner;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Will close all underlying resources
     *
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        groupWriter.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(Iterable<? extends BaseFeature> metricData) {
        try {
            for (BaseFeature metric : metricData) {

                AccumuloFeatureConfig featureConfig = registry.transformForClass(metric.getClass());

                if(featureConfig == null)
                    throw new RuntimeException("Skipping unknown feature type: " + metric.getClass());

                //fix null values
                String group = defaultString(metric.getGroup());
                String type = defaultString(metric.getType());
                String name = defaultString(metric.getName());
                ColumnVisibility visibility = new ColumnVisibility(defaultString(metric.getVisibility()));

                for (MetricTimeUnit timeUnit : MetricTimeUnit.values()) {

                    String timestamp = generateTimestamp(metric.getTimestamp(), timeUnit);
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
                            combine(featureConfig.featureName(), timeUnit.toString()),
                            combine(type, name),
                            visibility,
                            metric.getTimestamp(),
                            featureConfig.buildValue(metric)
                    );

                    //CF: Timeunit
                    //CQ: group\u0000name
                    type_mutation.put(
                            combine(featureConfig.featureName(), timeUnit.toString()),
                            combine(group, name),
                            visibility,
                            metric.getTimestamp(),
                            featureConfig.buildValue(metric)
                    );

                    groupWriter.addMutation(group_mutation);
                    typeWriter.addMutation(type_mutation);
                }
            }

            groupWriter.flush();
            typeWriter.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends BaseFeature>CloseableIterable<T> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Class<T> featureType,  Auths auths) {

        final AccumuloFeatureConfig<T> featureConfig = registry.transformForClass(featureType);

        return transform(
                closeableIterable(metricScanner(start, end, group, type, name, timeUnit, featureConfig, auths)),
                new FeatureEntryTransform<T>(timeUnit) {
                    @Override
                    protected T transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                        return featureConfig.buildFeatureFromValue(timestamp, group, type, name, visibility, value);
                    }
                }
        );
    }
}
