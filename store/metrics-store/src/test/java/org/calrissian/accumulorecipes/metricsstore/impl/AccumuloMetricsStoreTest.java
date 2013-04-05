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
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricType;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
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
    public void testPutAndQuery() throws TableNotFoundException {

        MetricUnit unit = new MetricUnit(System.currentTimeMillis(), "group", "type", "name", "",
                MetricType.COUNTER, 5l);

        store.put(Collections.singleton(unit));
        store.put(Collections.singleton(unit));

        Iterator<MetricUnit> itr = store.query(new Date(System.currentTimeMillis() - 500000),
                new Date(System.currentTimeMillis()), "group", "type", "name",
                MetricType.COUNTER, MetricTimeUnit.DAYS, new Authorizations());

        while(itr.hasNext()) {
            System.out.println(itr.next());
        }
    }


    protected void printTable() throws TableNotFoundException {

        Scanner scanner = connector.createScanner(store.getTableName(), new Authorizations());
        for(Map.Entry<Key,Value> entry : scanner) {
            System.out.println(entry);
        }
    }

}
