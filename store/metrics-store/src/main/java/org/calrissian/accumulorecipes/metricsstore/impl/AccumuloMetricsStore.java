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

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.metricsstore.MetricsContext;
import org.calrissian.accumulorecipes.metricsstore.MetricsStore;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;
import org.calrissian.accumulorecipes.metricsstore.normalizer.MetricNormalizer;
import org.calrissian.accumulorecipes.metricsstore.support.MetricsIterator;
import org.calrissian.accumulorecipes.metricsstore.support.TimestampUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

/**
 * An Accumulo implementation of the metrics store. This uses the versioning iterator to "continuously reduce" metrics
 * in buckets of time down to single counts. Further, functions can be applied against the metrics on the server side
 * to perform statistical analysis before the results come back to the client.
 */
public class AccumuloMetricsStore implements MetricsStore {

    Logger logger = LoggerFactory.getLogger(AccumuloMetricsStore.class);

    public static final String DELIM = "\u0000";
    public static final String M = "m";
    public static final String H = "h";
    public static final String D = "d";
    public static final String MO = "mo";

    protected String tableName = "metrics";

    protected Long maxMemory = 100000L;
    protected Integer numThreads = 3;
    protected Long maxLatency = 10000L;

    Connector connector;
    BatchWriter writer;

    public AccumuloMetricsStore(Connector connector) {
        this.connector = connector;
        try {
            this.writer = connector.createBatchWriter(tableName, maxMemory, maxLatency, numThreads);

            createTable();

        } catch (Exception e) {

            e.printStackTrace();
            logger.error("An error occurred initializing the Metrics Store. e=" + e);
        }
    }

    public void createTable() throws TableExistsException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

        if(!connector.tableOperations().exists(tableName)) {
            connector.tableOperations().create(tableName);

            Collection<IteratorUtil.IteratorScope> scopes = new ArrayList<IteratorUtil.IteratorScope>();
            scopes.add(IteratorUtil.IteratorScope.majc);
            scopes.add(IteratorUtil.IteratorScope.minc);
            scopes.add(IteratorUtil.IteratorScope.scan);

            EnumSet<IteratorUtil.IteratorScope> scope = EnumSet.copyOf(scopes);

            int priority = 5;
            for(MetricNormalizer normalizer : MetricsContext.getInstance().normalizers()) {

                IteratorSetting setting = new IteratorSetting(priority, normalizer.name(), normalizer.combinerClass());

                Method method = null;
                try {

                    method = normalizer.combinerClass().getMethod("setColumns", IteratorSetting.class, List.class);
                    List<IteratorSetting.Column> columns = Lists.newArrayList(
                            new IteratorSetting.Column(normalizer.name() + DELIM + M),
                            new IteratorSetting.Column(normalizer.name() + DELIM + H),
                            new IteratorSetting.Column(normalizer.name() + DELIM + D),
                            new IteratorSetting.Column(normalizer.name() + DELIM + MO)
                    );

                    normalizer.setSpecificIteratorOptions(setting);

                    method.invoke(null, setting, columns);
                    connector.tableOperations().attachIterator(tableName, setting, scope);
                    priority++;

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public Iterable<MetricUnit> query(Date start, Date end, String group, String type, String name, String metricType,
                                      MetricTimeUnit timeUnit, Authorizations auths) {

        try {

            String shortTimeUnit = parseTimeUnit(timeUnit);

            Scanner scanner = connector.createScanner(tableName, auths);

            Long stopRid = TimestampUtils.truncatedReverseTimestamp(start.getTime(), timeUnit);
            Long startRid = TimestampUtils.truncatedReverseTimestamp(end.getTime(), timeUnit);

            scanner.setRange(new Range(group + DELIM + startRid, group + DELIM + stopRid));
            scanner.fetchColumn(new Text(metricType + DELIM + shortTimeUnit), new Text(type + DELIM + name));

            return new MetricsIterator(scanner, metricType, timeUnit);

        } catch (TableNotFoundException e) {

            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<MetricUnit> metrics) {

        try {
            for(MetricUnit metricUnit : metrics) {

                String group = metricUnit.getGroup();
                String type = metricUnit.getType();
                String name = metricUnit.getName();

                Long timestamp = metricUnit.getTimestamp();

                MetricNormalizer normalizer = MetricsContext.getInstance()
                        .getNormalizer(metricUnit.getMetric().getClass());

                Value value = normalizer.getValue(metricUnit.getMetric());

                Long mTs = TimestampUtils.truncatedReverseTimestamp(timestamp, MetricTimeUnit.MINUTES);
                Mutation m = new Mutation(group + DELIM + mTs);
                m.put(new Text(normalizer.name() + DELIM + M),
                        new Text(type + DELIM + name), new ColumnVisibility(metricUnit.getVisibility()), timestamp, value);

                Long hTs = TimestampUtils.truncatedReverseTimestamp(timestamp, MetricTimeUnit.HOURS);
                Mutation h = new Mutation(group + DELIM + hTs);
                h.put(new Text(normalizer.name() + DELIM + H),
                        new Text(type + DELIM + name), new ColumnVisibility(metricUnit.getVisibility()), timestamp, value);

                Long dTs = TimestampUtils.truncatedReverseTimestamp(timestamp, MetricTimeUnit.DAYS);
                Mutation d = new Mutation(group + DELIM + dTs);
                d.put(new Text(normalizer.name() + DELIM + D),
                        new Text(type + DELIM + name), new ColumnVisibility(metricUnit.getVisibility()), timestamp, value);

                Long moTs = TimestampUtils.truncatedReverseTimestamp(timestamp, MetricTimeUnit.MONTHS);
                Mutation mo = new Mutation(metricUnit.getGroup() + DELIM + moTs);
                mo.put(new Text(normalizer.name() + DELIM + MO),
                        new Text(type + DELIM + name), new ColumnVisibility(metricUnit.getVisibility()), timestamp, value);

                writer.addMutations(Arrays.asList(new Mutation[]{m, d, h, mo}));
            }

            writer.flush();

        } catch(Exception e) {

            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private String parseTimeUnit(MetricTimeUnit timeUnit) {
        String shortTimeUnit = M;
        switch(timeUnit) {
            case DAYS:
                shortTimeUnit = D;
                break;
            case HOURS:
                shortTimeUnit = H;
                break;
            case MONTHS:
                shortTimeUnit = M;
        }

        return shortTimeUnit;
    }

    @Override
    public void shutdown() {

    }

    public Long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(Long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public Integer getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(Integer numThreads) {
        this.numThreads = numThreads;
    }

    public Long getMaxLatency() {
        return maxLatency;
    }

    public void setMaxLatency(Long maxLatency) {
        this.maxLatency = maxLatency;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
