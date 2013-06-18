package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.CustomMetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.iterator.FunctionCombiner;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;

import java.util.Date;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.metricsstore.support.TimestampUtil.revertTimestamp;


/**
 * This implementation of the metric service that allows the caller to specify a custom function during query time.  No
 * table configured iterators are configured for this table, as this might skew the results for any custom function.
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

    private static final String DEFAULT_TABLE_NAME = "custom_metrics";

    public AccumuloCustomMetricStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, DEFAULT_TABLE_NAME);
    }

    public AccumuloCustomMetricStore(Connector connector, String tableName) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName);
    }

    @Override
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //do nothing as scan time iterators are attached at query time for a specific custom metric.
    }

    @Override
    public Iterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Authorizations auths) {
        return emptyList();  //no way to guarantee that a single value can be retrieved, so simply return an empty iterable.
    }

    @Override
    public <T> Iterable<CustomMetric<T>> queryCustom(Date start, Date end, String group, String type, String name, Class<? extends MetricFunction<T>> function, MetricTimeUnit timeUnit, Authorizations auths) throws IllegalAccessException, InstantiationException {
        checkNotNull(function);

        MetricFunction<T> impl = function.newInstance();

        Scanner scanner = metricScanner(start, end, group, type, name, timeUnit, auths);

        //Add a scan time iterator to apply the custom function.
        IteratorSetting setting = new IteratorSetting(10, "functionCombiner", FunctionCombiner.class);
        FunctionCombiner.setFunctionClass(setting, function);
        FunctionCombiner.setColumns(setting, asList(new Column(timeUnit.toString())));
        scanner.addScanIterator(setting);


        return transform(scanner, new CustomMetricTransform<T>(impl, timeUnit));
    }

    /**
     * Utility class to help provide the transform logic to go from the Entry<Key, Value> from accumulo to the Metric
     * objects that are returned from this service.
     */
    private static class CustomMetricTransform<T> implements Function<Map.Entry<Key, Value>, CustomMetric<T>> {
        MetricFunction<T> function;
        MetricTimeUnit timeUnit;

        private CustomMetricTransform(MetricFunction<T> function, MetricTimeUnit timeUnit) {
            this.function = function;
            this.timeUnit = timeUnit;
        }

        @Override
        public CustomMetric<T> apply(Map.Entry<Key, Value> entry) {

            String row[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), DELIM);
            String colQ[] = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), DELIM);
            T value = function.deserialize(entry.getValue().toString().substring(1));

            return new CustomMetric<T>(
                    revertTimestamp(row[1], timeUnit),
                    row[0],
                    colQ[0],
                    colQ[1],
                    entry.getKey().getColumnVisibility().toString(),
                    value
            );
        }
    }
}
