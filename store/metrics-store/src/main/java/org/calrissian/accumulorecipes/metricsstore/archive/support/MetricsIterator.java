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
package org.calrissian.accumulorecipes.metricsstore.archive.support;


import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.calrissian.accumulorecipes.metricsstore.archive.MetricsContext;
import org.calrissian.accumulorecipes.metricsstore.archive.domain.MetricUnit;
import org.calrissian.accumulorecipes.metricsstore.archive.normalizer.MetricNormalizer;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;

import java.util.Iterator;
import java.util.Map;

import static org.calrissian.accumulorecipes.metricsstore.archive.impl.AccumuloMetricsStore.DELIM;

public class MetricsIterator implements Iterator<MetricUnit>, Iterable<MetricUnit> {

    Scanner scanner;
    Iterator<Map.Entry<Key,Value>> itr;
    MetricTimeUnit timeUnit;
    String metricType;

    public MetricsIterator(Scanner scanner, String metricType, MetricTimeUnit Timeunit) {

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

        try {
            if(hasNext()) {

                Map.Entry<Key,Value> entry = itr.next();

                String row[] = entry.getKey().getRow().toString().split(DELIM);
                String colF[] = entry.getKey().getColumnFamily().toString().split(DELIM);
                String colQ[] = entry.getKey().getColumnQualifier().toString().split(DELIM);

                String group = row[0];
                String type = colQ[0];
                String name = colQ[1];

                MetricNormalizer normalizer = MetricsContext.getInstance().getNormalizer(metricType);


                return new MetricUnit(entry.getKey().getTimestamp(), group, type, name,
                        entry.getKey().getColumnVisibility().toString(),normalizer.getMetric(entry.getValue()));
            }

            else {
                throw new IterationInterruptedException("No more entries left");
            }
        }

        catch(Exception e) {
            throw new IterationInterruptedException(e.toString());
        }
    }

    @Override
    public void remove() {
        itr.remove();
    }

    @Override
    public Iterator<MetricUnit> iterator() {
        return new MetricsIterator(scanner, metricType, timeUnit);
    }
}
