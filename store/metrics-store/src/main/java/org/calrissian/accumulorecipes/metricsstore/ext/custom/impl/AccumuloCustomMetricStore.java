package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.CustomMetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.iterator.FunctionCombiner;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;
import org.calrissian.accumulorecipes.metricsstore.support.MetricTransform;

import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DEFAULT_ITERATOR_PRIORITY;

/**
 * This implementation of the metric service allows the caller to specify a custom function during query time.  No
 * table configured iterators are setup for this table, as this might skew the results for any custom function.
 * This means that during a compaction, the table is not able to consolidate data possibly increasing the query times for
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

    public AccumuloCustomMetricStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, "custom_metrics");
    }

    public AccumuloCustomMetricStore(Connector connector, String tableName) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //do nothing as scan time iterators are attached at query time for a specific custom metric.
        //this prevents the values in the table from getting squashed, so that different types of
        //metric calculations can be run at any time.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Authorizations auths) {

        Scanner scanner = metricScanner(start, end, group, type, name, timeUnit, auths);

        //Add a scan time SummingCombiner
        IteratorSetting setting  = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, "stats", SummingCombiner.class);
        SummingCombiner.setColumns(setting, asList(new Column(timeUnit.toString())));
        SummingCombiner.setEncodingType(setting, LongCombiner.Type.STRING);
        scanner.addScanIterator(setting);

        return transform(
                scanner,
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
    public <T> Iterable<CustomMetric<T>> queryCustom(Date start, Date end, String group, String type, String name, Class<? extends MetricFunction<T>> function, MetricTimeUnit timeUnit, Authorizations auths) throws IllegalAccessException, InstantiationException {
        checkNotNull(function);

        final MetricFunction<T> impl = function.newInstance();

        Scanner scanner = metricScanner(start, end, group, type, name, timeUnit, auths);

        //Add a scan time iterator to apply the custom function.
        IteratorSetting setting = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, "functionCombiner", FunctionCombiner.class);
        FunctionCombiner.setFunctionClass(setting, function);
        FunctionCombiner.setColumns(setting, asList(new Column(timeUnit.toString())));
        scanner.addScanIterator(setting);

        return transform(scanner, new CustomMetricTransform<T>(timeUnit, impl));
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
            return new CustomMetric<T>(
                    timestamp,
                    group,
                    type,
                    name,
                    visibility,
                    function.deserialize(value.toString().substring(1))
            );
        }
    }
}
