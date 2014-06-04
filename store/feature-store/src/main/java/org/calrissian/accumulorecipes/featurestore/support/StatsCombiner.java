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
package org.calrissian.accumulorecipes.featurestore.support;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;

import static java.lang.Long.parseLong;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * This combiner calculates the max, min, sum, count, and sumSquare of long integers represented as strings in values. It stores the result in a comma-separated value of
 * the form max,min,sum,count,sumSquare. If such a value is encountered while combining, its information is incorporated into the running calculations of max, min, sum,
 * ,count, and sumSquare. See {@link Combiner} for more information on which values are combined together.
 */
public class StatsCombiner extends Combiner {

    private String join(String seperator, String... values) {
        return StringUtils.join(values, seperator);
    }

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0;
        long count = 0;
        long sumSquare = 0;

        while (iter.hasNext()) {

            String stats[] = iter.next().toString().split(",");

            if (stats.length == 1) {
                long val = parseLong(stats[0]);
                min = min(val, min);
                max = max(val, max);
                sum += val;
                count += 1;
                sumSquare += (val * val);
            } else {
                min = min(parseLong(stats[0]), min);
                max = max(parseLong(stats[1]), max);
                sum += parseLong(stats[2]);
                count += parseLong(stats[3]);
                sumSquare += parseLong(stats[4]);
            }
        }

        String ret = join(",",
                Long.toString(min),
                Long.toString(max),
                Long.toString(sum),
                Long.toString(count),
                Long.toString(sumSquare)
        );

        return new Value(ret.getBytes());
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("statsCombiner");
        io.setDescription("Combiner that keeps track of min, max, sum, and count");
        return io;
    }
}