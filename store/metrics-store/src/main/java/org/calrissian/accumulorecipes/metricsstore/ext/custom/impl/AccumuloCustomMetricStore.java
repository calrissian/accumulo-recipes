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
package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.CustomMetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.iterator.FunctionCombiner;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;
import org.calrissian.accumulorecipes.metricsstore.support.MetricTransform;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;

/**
 * This implementation of the metric service allows the caller to specify a custom function during criteria time.  No
 * table configured iterators are setup for this table, as this might skew the results for any custom function.
 * This means that during a compaction, the table is not able to consolidate data possibly increasing the criteria times for
 * metric data.
 *
 *
 * Format of the table:
 * Rowid                CF                  CQ                  Value
 * group\u0000revTS     'MINUTES'           type\u0000name      value
 * group\u0000revTS     'HOURS'             type\u0000name      value
 * group\u0000revTS     'DAYS'              type\u0000name      value
 * group\u0000revTS     'MONTHS'            type\u0000name      value
 */
public class AccumuloCustomMetricStore extends AccumuloMetricStore implements CustomMetricStore {

    private static final String DEFAULT_TABLE_NAME = "custom_metrics";

    public AccumuloCustomMetricStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloCustomMetricStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //do nothing as scan time iterators are attached at criteria time for a specific custom metric.
        //this prevents the values in the table from getting squashed, so that different types of
        //metric calculations can be run at any time.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableIterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Auths auths) {

        ScannerBase scanner = metricScanner(start, end, group, type, name, timeUnit, auths);

        //Add a scan time SummingCombiner
        IteratorSetting setting  = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, "stats", SummingCombiner.class);
        SummingCombiner.setColumns(setting, asList(new Column(timeUnit.toString())));
        SummingCombiner.setEncodingType(setting, LongCombiner.Type.STRING);
        scanner.addScanIterator(setting);

        return transform(
                closeableIterable(scanner),
                new MetricTransform<Metric>(timeUnit) {
                    @Override
                    protected Metric transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                        return new Metric(timestamp, group, type, name, visibility, parseLong(value.toString()));
                    }
                }
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> CloseableIterable<CustomMetric<T>> queryCustom(Date start, Date end, String group, String type, String name, Class<? extends MetricFunction<T>> function, MetricTimeUnit timeUnit, Auths
            auths) throws IllegalAccessException, InstantiationException {
        checkNotNull(function);

        final MetricFunction<T> impl = function.newInstance();

        ScannerBase scanner = metricScanner(start, end, group, type, name, timeUnit, auths);

        //Add a scan time iterator to apply the custom function.
        IteratorSetting setting = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, "functionCombiner", FunctionCombiner.class);
        FunctionCombiner.setFunctionClass(setting, function);
        FunctionCombiner.setColumns(setting, asList(new Column(timeUnit.toString())));
        scanner.addScanIterator(setting);

        return transform(closeableIterable(scanner), new CustomMetricTransform<T>(timeUnit, impl));
    }

    /**
     * Utility class to help provide the transform logic to go from the Entry<Key, Value> from accumulo to the Metric
     * objects that are returned from this store.
     */
    private static class CustomMetricTransform<T> extends MetricTransform<CustomMetric<T>> {
        MetricFunction<T> function;

        public CustomMetricTransform(MetricTimeUnit timeUnit, MetricFunction<T> function) {
            super(timeUnit);
            this.function = function;
        }

        @Override
        protected CustomMetric<T> transform(long timestamp, String group, String type, String name, String visibility, Value value) {
            byte[] data = value.get();
            return new CustomMetric<T>(
                    timestamp,
                    group,
                    type,
                    name,
                    visibility,
                    function.deserialize(copyOfRange(data, 1, data.length))
            );
        }
    }
}
