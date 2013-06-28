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
package org.calrissian.accumulorecipes.metricsstore.impl;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.support.MetricTransform;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Long.parseLong;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.metricsstore.support.TimestampUtil.generateTimestamp;

/**
 * This class will store simple metric data into accumulo.  The metrics will aggregate over predefined time intervals
 * but are available immediately to query.
 *
 * Format of the table:
 * Rowid                CF                  CQ                  Value
 * group\u0000revTS     'MINUTES'           type\u0000name      value
 * group\u0000revTS     'HOURS'             type\u0000name      value
 * group\u0000revTS     'DAYS'              type\u0000name      value
 * group\u0000revTS     'MONTHS'            type\u0000name      value
 *
 * The table is configured to use a SummingCombiner against each of the columns specified.
 */
public class AccumuloMetricStore implements MetricStore {

    private final Connector connector;
    private final String tableName;
    private final BatchWriter metricWriter;

    public AccumuloMetricStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this(connector, "metrics");
    }

    public AccumuloMetricStore(Connector connector, String tableName) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        checkNotNull(connector);
        checkNotNull(tableName);

        this.connector = connector;
        this.tableName = tableName;

        if(!connector.tableOperations().exists(this.tableName)) {
            //Create table without versioning iterator.
            connector.tableOperations().create(this.tableName, false);
            configureTable(connector, this.tableName);
        }

        this.metricWriter = this.connector.createBatchWriter(tableName, 100000, 100, 10);
    }

    protected static String combine(String... items) {
        if (items == null)
            return null;
        return join(items, DELIM);
    }

    /**
     * Utility method to update the correct iterators to the table.
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Set up the default SummingCombiner
        List<Column> columns = new ArrayList<Column>();
        for (MetricTimeUnit timeUnit : MetricTimeUnit.values())
            columns.add(new Column(timeUnit.toString()));

        IteratorSetting setting  = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, "stats", SummingCombiner.class);
        SummingCombiner.setColumns(setting, columns);
        SummingCombiner.setEncodingType(setting, LongCombiner.Type.STRING);
        connector.tableOperations().attachIterator(tableName, setting, allOf(IteratorScope.class));
    }

    protected Scanner metricScanner(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Auths auths) {
        checkNotNull(start);
        checkNotNull(end);
        checkNotNull(auths);

        try {

            //fix null values
            group = defaultString(group);
            timeUnit = (timeUnit == null ? MetricTimeUnit.MINUTES : timeUnit);

            //Start scanner over the known range group_end to group_start.  The order is reversed due to the use of a reverse
            //timestamp.  Which is used to provide the latest results first.
            Scanner scanner = connector.createScanner(tableName, auths.getAuths());
            scanner.setRange(new Range(
                    combine(group, generateTimestamp(end.getTime(), timeUnit)),
                    combine(group, generateTimestamp(start.getTime(), timeUnit))
            ));

            //If both type and name are here then simply fetch the column
            //else fetch the timeunit column family and apply a regex filter for the CQ containing the type and name.
            if (type != null && name != null) {
                scanner.fetchColumn(new Text(timeUnit.toString()), new Text(combine(type, name)));
            } else {
                scanner.fetchColumnFamily(new Text(timeUnit.toString()));

                //generates the correct regex
                String cqRegex = null;
                if (type != null)
                    cqRegex = combine(type, "(.*)");
                else if (name != null)
                    cqRegex = combine("(.*)", name);

                //No need to apply a filter if there is no regex to apply.
                if (cqRegex != null) {
                    IteratorSetting regexIterator = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY - 1, "regex", RegExFilter.class);
                    RegExFilter.setRegexs(regexIterator, null, null, cqRegex, null, false);
                    scanner.addScanIterator(regexIterator);
                }
            }

            return scanner;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Will close all underlying resources
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        metricWriter.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(Iterable<Metric> metricData) {
        try {
            for (Metric metric : metricData) {
                if (metric == null)
                    continue;

                //fix null values
                String group = defaultString(metric.getGroup());
                String type = defaultString(metric.getType());
                String name = defaultString(metric.getName());
                ColumnVisibility visibility = new ColumnVisibility(defaultString(metric.getVisibility()));

                for (MetricTimeUnit timeUnit : MetricTimeUnit.values()) {

                    //Create mutation with:
                    //rowID: group\u0000timestamp
                    Mutation mutation = new Mutation(
                            combine(group, generateTimestamp(metric.getTimestamp(), timeUnit))
                    );

                    //CF: Timeunit
                    //CQ: type\u0000name
                    mutation.put(
                            timeUnit.toString(),
                            combine(type, name),
                            visibility,
                            metric.getTimestamp(),
                            new Value(Long.toString(metric.getValue()).getBytes())
                    );

                    metricWriter.addMutation(mutation);
                }
            }

            metricWriter.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Auths auths) {
        return transform(
                metricScanner(start, end, group, type, name, timeUnit, auths),
                new MetricTransform<Metric>(timeUnit) {
                    @Override
                    protected Metric transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                        return new Metric(timestamp, group, type, name, visibility, parseLong(value.toString()));
                    }
                }
        );
    }
}
