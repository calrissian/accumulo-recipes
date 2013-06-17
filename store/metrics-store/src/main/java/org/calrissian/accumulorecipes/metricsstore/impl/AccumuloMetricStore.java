package org.calrissian.accumulorecipes.metricsstore.impl;


import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.iterator.StatsCombiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.EnumSet.allOf;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.commons.lang.StringUtils.*;
import static org.calrissian.accumulorecipes.metricsstore.support.TimestampUtil.generateTimestamp;
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
 * The table is configured to use a Stats combiner against each of the columns specified.
 */
public class AccumuloMetricStore implements MetricStore {

    protected static final String DELIM = "\u0000";
    private static final String DEFAULT_TABLE_NAME = "metrics";

    private final Connector connector;
    private final String tableName;
    private final BatchWriter metricWriter;

    public AccumuloMetricStore(Connector connector) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this(connector, DEFAULT_TABLE_NAME);
    }

    public AccumuloMetricStore(Connector connector, String tableName) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        checkNotNull(connector);
        checkNotNull(tableName);

        this.connector = connector;
        this.tableName = tableName;

        createTable(this.connector);

        this.metricWriter = this.connector.createBatchWriter(tableName, 100000, 100, 10);
    }

    protected static String combine(String... items) {
        if (items == null)
            return null;
        return join(items, DELIM);
    }

    /**
     * Utility method to create the table if it does not exist.
     * @param connector
     * @throws org.apache.accumulo.core.client.TableExistsException
     * @throws org.apache.accumulo.core.client.AccumuloSecurityException
     * @throws org.apache.accumulo.core.client.AccumuloException
     * @throws org.apache.accumulo.core.client.TableNotFoundException
     */
    private void createTable(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        if(!connector.tableOperations().exists(this.tableName)) {
            //Create table without versioning iterator.
            connector.tableOperations().create(tableName, false);

            configureTable(connector, tableName);
        }

    }

    /**
     * Utility method to add the correct iterators to the table.
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Set up the default summing combiner with a priority of 30
        List<Column> columns = new ArrayList<Column>();
        for (MetricTimeUnit timeUnit : MetricTimeUnit.values())
            columns.add(new Column(timeUnit.toString()));

        IteratorSetting setting  = new IteratorSetting(30, "stats", StatsCombiner.class);
        StatsCombiner.setColumns(setting, columns);
        connector.tableOperations().attachIterator(tableName, setting, allOf(IteratorScope.class));
    }

    /**
     * Utility method to retrieve all metric values from the table pre-transform.
     * @param start
     * @param end
     * @param group
     * @param type
     * @param name
     * @param timeUnit
     * @param auths
     * @return
     */
    protected Iterable<Entry<Key, Value>> queryInternal(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Collection<String> auths) {
        checkNotNull(start);
        checkNotNull(end);
        checkNotNull(auths);

        try {

            //fix null values
            group = (group == null ? "" : group);
            timeUnit = (timeUnit == null ? MetricTimeUnit.MINUTES : timeUnit);

            //Start scanner over the known range group_end to group_start.  The order is reversed due to the use of a reverse
            //timestamp.  Which is used to provide the latest results first.
            Scanner scanner = connector.createScanner(tableName, new Authorizations(auths.toArray(new String[auths.size()])));
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
                    IteratorSetting regexIterator = new IteratorSetting(50, "regex", RegExFilter.class);
                    RegExFilter.setRegexs(regexIterator, null, null, cqRegex, null, false);
                    scanner.addScanIterator(regexIterator);
                }
            }

            //Use a transform to convert from Entry<Key,Value> to Metric
            return scanner;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public Iterable<Metric> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Collection<String> auths) {

        return transform(
                queryInternal(start, end, group, type, name, timeUnit, auths),
                new MetricTransform(timeUnit)
        );
    }

    /**
     * Utility class to help provide the transform logic to go from the Entry<Key, Value> from accumulo to the Metric
     * objects that are returned from this service.
     */
    private static class MetricTransform implements Function<Entry<Key, Value>, Metric> {
        MetricTimeUnit timeUnit;

        private MetricTransform(MetricTimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public Metric apply(Entry<Key, Value> entry) {

            String row[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), DELIM);
            String colQ[] = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), DELIM);

            return new Metric(
                    revertTimestamp(row[1], timeUnit),
                    row[0],
                    colQ[0],
                    colQ[1],
                    entry.getKey().getColumnVisibility().toString(),
                    Long.parseLong(entry.getValue().toString())
            );
        }
    }
}
