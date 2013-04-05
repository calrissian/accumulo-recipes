/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.metricsstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.MetricsContext;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.impl.CounterMetric;
import org.calrissian.accumulorecipes.metricsstore.domain.impl.StatsMetric;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class AccumuloMetricsStoreTest {


    Connector connector;
    AccumuloMetricsStore store;

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {
        Instance instance = new MockInstance();
        this.connector = instance.getConnector("root", "password".getBytes());

        this.store = new AccumuloMetricsStore(connector);
    }

    @Test
    public void testPutAndQueryForCounterMetric() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

        Metric metric = new CounterMetric(5l);

        MetricUnit unit = new MetricUnit(System.currentTimeMillis(), "group", "type", "name", "", metric);

        store.put(Collections.singleton(unit));
        store.put(Collections.singleton(unit));

        Iterable<MetricUnit> itr = store.query(new Date(System.currentTimeMillis() - 500000),
                new Date(System.currentTimeMillis()), "group", "type", "name",
                MetricsContext.getInstance().getNormalizer(metric.getClass()).name(),
                MetricTimeUnit.DAYS, new Authorizations());

        for(MetricUnit result : itr) {
            System.out.println(result);
        }
    }

    @Test
    public void testPutAndQueryForStatsMetric() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

        Metric metric = new StatsMetric(5l, 5l, 5l, 5l);
        Metric metric2 = new StatsMetric(1l, 3l, 5l, 9l);

        MetricUnit unit = new MetricUnit(System.currentTimeMillis(), "group", "type", "name", "", metric);
        MetricUnit unit2 = new MetricUnit(System.currentTimeMillis(), "group", "type", "name", "", metric2);

        store.put(Collections.singleton(unit));
        store.put(Collections.singleton(unit2));

        Iterable<MetricUnit> itr = store.query(new Date(System.currentTimeMillis() - 500000),
                new Date(System.currentTimeMillis()), "group", "type", "name",
                MetricsContext.getInstance().getNormalizer(metric.getClass()).name(),
                MetricTimeUnit.DAYS, new Authorizations());

        for(MetricUnit result : itr) {
            System.out.println(result);
        }
    }



    protected void printTable() throws TableNotFoundException {

        Scanner scanner = connector.createScanner(store.getTableName(), new Authorizations());
        for(Map.Entry<Key,Value> entry : scanner) {
            System.out.println(entry);
        }
    }

}
