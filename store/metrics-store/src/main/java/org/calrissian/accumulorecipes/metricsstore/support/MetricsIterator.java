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
package org.calrissian.accumulorecipes.metricsstore.support;


import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricType;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;

import java.util.Iterator;
import java.util.Map;

import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricsStore.DELIM;

public class MetricsIterator implements Iterator<MetricUnit> {

    Scanner scanner;
    Iterator<Map.Entry<Key,Value>> itr;
    MetricTimeUnit timeUnit;
    MetricType metricType;

    public MetricsIterator(Scanner scanner, MetricType metricType, MetricTimeUnit Timeunit) {

        this.scanner = scanner;
        this.itr = scanner.iterator();
        this.timeUnit = timeUnit;
        this.metricType = metricType;
    }
    @Override
    public boolean hasNext() {
        return itr.hasNext();
    }

    @Override
    public MetricUnit next() {

        if(hasNext()) {

            Map.Entry<Key,Value> entry = itr.next();

            String row[] = entry.getKey().getRow().toString().split(DELIM);
            String colF[] = entry.getKey().getColumnFamily().toString().split(DELIM);
            String colQ[] = entry.getKey().getColumnQualifier().toString().split(DELIM);

            String group = row[0];
            String type = colQ[0];
            String name = colQ[1];


            return new MetricUnit(entry.getKey().getTimestamp(), group, type, name,
                    entry.getKey().getColumnVisibility().toString(), metricType,
                    Long.parseLong(new String(entry.getValue().get())));
        }

        else {
            throw new IterationInterruptedException("No more entries left");
        }
    }

    @Override
    public void remove() {
        itr.remove();
    }
}
