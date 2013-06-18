package org.calrissian.accumulorecipes.metricsstore.ext.stats.impl;


import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.StatsMetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.domain.Stats;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.iterator.StatsCombiner;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;
import org.calrissian.accumulorecipes.metricsstore.support.MetricTransform;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static java.lang.Long.parseLong;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;

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
        super(connector, DEFAULT_TABLE_NAME);
    }

    public AccumuloStatsMetricStore(Connector connector, String tableName) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName);
    }

    @Override
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Set up the default StatsCombiner
        List<Column> columns = new ArrayList<Column>();
        for (MetricTimeUnit timeUnit : MetricTimeUnit.values())
            columns.add(new Column(timeUnit.toString()));

        IteratorSetting setting  = new IteratorSetting(10, "stats", StatsCombiner.class);
        StatsCombiner.setColumns(setting, columns);
        connector.tableOperations().attachIterator(tableName, setting, allOf(IteratorScope.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Authorizations auths) {
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
    public Iterable<Stats> queryStats(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Authorizations auths) {

        return transform(
                metricScanner(start, end, group, type, name, timeUnit, auths),
                new MetricStatsTransform(timeUnit)
        );
    }

    /**
     * Utility class to help provide the transform logic to go from the Entry<Key, Value> from accumulo to the Metric
     * objects that are returned from this service.
     */
    private static class MetricStatsTransform extends MetricTransform<Stats> {
        MetricTimeUnit timeUnit;

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
                    parseLong(values[3]));
        }
    }
}
