package org.calrissian.accumulorecipes.metricsstore.ext.impl;


import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.StatsMetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.domain.Stats;
import org.calrissian.accumulorecipes.metricsstore.ext.iterator.StatsCombiner;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static java.lang.Long.parseLong;
import static java.util.EnumSet.allOf;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.metricsstore.support.TimestampUtil.revertTimestamp;

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
                queryInternal(start, end, group, type, name, timeUnit, auths),
                new MetricStatsTransform(timeUnit)
        );
    }

    /**
     * Utility class to help provide the transform logic to go from the Entry<Key, Value> from accumulo to the Metric
     * objects that are returned from this service.
     */
    private static class MetricStatsTransform implements Function<Entry<Key, Value>, Stats> {
        MetricTimeUnit timeUnit;

        private MetricStatsTransform(MetricTimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public Stats apply(Entry<Key, Value> entry) {

            String row[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), DELIM);
            String colQ[] = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), DELIM);
            String value[] = splitPreserveAllTokens(entry.getValue().toString(), ",");

            return new Stats(
                    revertTimestamp(row[1], timeUnit),
                    row[0],
                    colQ[0],
                    colQ[1],
                    entry.getKey().getColumnVisibility().toString(),
                    parseLong(value[0]),
                    parseLong(value[1]),
                    parseLong(value[2]),
                    parseLong(value[3])
            );
        }
    }
}
