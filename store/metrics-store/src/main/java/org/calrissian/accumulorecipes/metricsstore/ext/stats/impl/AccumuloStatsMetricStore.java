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
package org.calrissian.accumulorecipes.metricsstore.ext.stats.impl;


import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.StatsMetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.domain.Stats;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.iterator.StatsCombiner;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;
import org.calrissian.accumulorecipes.metricsstore.support.MetricTransform;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Long.parseLong;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;

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
 * The table is configured to use a StatsCombiner against each of the columns specified.
 */
public class AccumuloStatsMetricStore extends AccumuloMetricStore implements StatsMetricStore {

    private static final String DEFAULT_TABLE_NAME = "stats_metrics";

    public AccumuloStatsMetricStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloStatsMetricStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Set up the default StatsCombiner
        List<Column> columns = new ArrayList<Column>();
        for (MetricTimeUnit timeUnit : MetricTimeUnit.values())
            columns.add(new Column(timeUnit.toString()));

        IteratorSetting setting  = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, "stats", StatsCombiner.class);
        StatsCombiner.setColumns(setting, columns);
        connector.tableOperations().attachIterator(tableName, setting, allOf(IteratorScope.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableIterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Auths auths) {
        return transform(
                queryStats(start, end, group, type, name, timeUnit, auths),
                new Function<Stats, Metric>() {
                    @Override
                    public Metric apply(Stats stat) {
                        return new Metric(
                                stat.getTimestamp(),
                                stat.getGroup(),
                                stat.getType(),
                                stat.getName(),
                                stat.getVisibility(),
                                stat.getSum()
                        );
                    }
                }
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableIterable<Stats> queryStats(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Auths auths) {

        return transform(
                closeableIterable(metricScanner(start, end, group, type, name, timeUnit, auths)),
                new MetricStatsTransform(timeUnit)
        );
    }

    /**
     * Utility class to help provide the transform logic to go from the Entry<Key, Value> from accumulo to the Metric
     * objects that are returned from this store.
     */
    private static class MetricStatsTransform extends MetricTransform<Stats> {

        public MetricStatsTransform(MetricTimeUnit timeUnit) {
            super(timeUnit);
        }

        @Override
        protected Stats transform(long timestamp, String group, String type, String name, String visibility, Value value) {
            String values[] = splitPreserveAllTokens(value.toString(), ",");
            return new Stats(
                    timestamp,
                    group,
                    type,
                    name,
                    visibility,
                    parseLong(values[0]),
                    parseLong(values[1]),
                    parseLong(values[2]),
                    parseLong(values[3]),
                    parseLong(values[4]));
        }
    }
}
