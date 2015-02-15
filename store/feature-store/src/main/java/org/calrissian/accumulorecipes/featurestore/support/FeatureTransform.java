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
import org.calrissian.accumulorecipes.featurestore.model.Feature;

import static java.util.Map.Entry;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.util.TimestampUtil.revertTimestamp;

/**
 * Simple utility class to extract the metadata from a Key/Value to allow specific Metric types to be returned.
 *
 * @param <T>
 */
public abstract class FeatureTransform<T extends Feature> implements Function<Entry<Key, Value>, T> {

    protected abstract T transform(long timestamp, String group, String type, String name, String visibility, Value value);

    @Override
    public T apply(Entry<Key, Value> entry) {

        String row[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), NULL_BYTE);
        String colQ[] = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), NULL_BYTE);

        return transform(
                revertTimestamp(row[1]),
                colQ[0],
                row[0],
                colQ[1],
                entry.getKey().getColumnVisibility().toString(),
                entry.getValue()
        );

    }
}
