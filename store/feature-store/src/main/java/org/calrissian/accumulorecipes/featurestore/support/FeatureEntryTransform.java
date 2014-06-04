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

import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;

import static java.util.Map.Entry;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.revertTimestamp;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DELIM;

/**
 * Simple utility class to extract the metadata from a Key/Value to allow specific Metric types to be returned.
 *
 * @param <T>
 */
public abstract class FeatureEntryTransform<T> implements Function<Entry<Key, Value>, T> {

    MetricTimeUnit timeUnit;

    public FeatureEntryTransform(MetricTimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    protected abstract T transform(long timestamp, String group, String type, String name, String visibility, Value value);

    @Override
    public T apply(Entry<Key, Value> entry) {

        String row[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), DELIM);
        String colQ[] = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), DELIM);

        return transform(
                revertTimestamp(row[1], timeUnit),
                colQ[0],
                row[0],
                colQ[1],
                entry.getKey().getColumnVisibility().toString(),
                entry.getValue()
        );

    }
}
