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
package org.calrissian.accumulorecipes.featurestore.hadoop;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.Feature;
import org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry;
import org.calrissian.accumulorecipes.featurestore.support.FeatureTransform;
import org.calrissian.accumulorecipes.featurestore.support.config.AccumuloFeatureConfig;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore.DEFAULT_TABLE_NAME;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry.BASE_FEATURES;
import static org.calrissian.accumulorecipes.featurestore.support.Utilities.combine;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;

/**
 * A Hadoop {@link InputFormat} that allows any Feature to be streamed into a map/reduce job based on a given query.
 */
public class FeaturesInputFormat extends InputFormatBase<Key, Feature> {

    public static void setInputInfo(Job job, String username, byte[] password, Authorizations auths) throws AccumuloSecurityException {
        setConnectorInfo(job, username, new PasswordToken(password));
        setInputTableName(job,DEFAULT_TABLE_NAME);
        setScanAuthorizations(job, auths);
    }

    public static void setQueryInfo(Job job, Date start, Date end, TimeUnit timeUnit, String group, String type, String name, Class<? extends Feature> featureType) throws IOException {
        setQueryInfo(job, start, end, timeUnit, group, type, name, featureType, BASE_FEATURES);
    }

    /**
     * Query for a specific set of feature rollups for a specific time range and unit of time. At a minimum, a group
     * needs to be specified. Type and name are optional. The requested feature type to query determines the vector
     * that will be streamed into the map/reduce job. A registry allows pluggable feature types to be propagated down
     * to a store where features have been ingested with matching feature configs.
     *
     * NOTE: It can be dangerous to apply a registry to a feature
     */
    public static void setQueryInfo(Job job, Date start, Date end, TimeUnit timeUnit, String group, String type, String name, Class<? extends Feature> featureType, FeatureRegistry registry) throws IOException {

        AccumuloFeatureConfig featureConfig = registry.transformForClass(featureType);

        job.getConfiguration().set("featureConfig", new String(toBase64(featureConfig)));
        job.getConfiguration().set("timeUnit", timeUnit.toString());

        setRanges(job,
                singleton(new Range(
                    combine(group, generateTimestamp(end.getTime(), timeUnit)),
                    combine(group, generateTimestamp(start.getTime(), timeUnit))
                ))
        );

        if (name != null) {
            Pair<Text, Text> column = new Pair<Text, Text>(new Text(combine(timeUnit.toString(), featureConfig.featureName())), new Text(combine(type, name)));
            fetchColumns(job, singleton(column));
        } else {
            Pair<Text, Text> column = new Pair<Text, Text>(new Text(combine(timeUnit.toString(), featureConfig.featureName())), null);
            fetchColumns(job, singleton(column));

            if(type != null) {
                String cqRegex = null;
                cqRegex = combine(type, "(.*)");
                IteratorSetting regexIterator = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY - 1, "regex", RegExFilter.class);
                RegExFilter.setRegexs(regexIterator, null, null, cqRegex, null, false);
                addIterator(job, regexIterator);
            }
        }
    }

    @Override
    public RecordReader<Key, Feature> createRecordReader(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {

        TimeUnit timeUnit = TimeUnit.valueOf(context.getConfiguration().get("timeUnit"));

        try {
            final AccumuloFeatureConfig<? extends Feature> config = fromBase64(context.getConfiguration().get("featureConfig").getBytes());
            final FeatureTransform<? extends Feature> entryTransform = new FeatureTransform<Feature>(timeUnit) {
                @Override
                protected Feature transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                    return config.buildFeatureFromValue(timestamp, type, group, name, visibility, value);
                }
            };

            return new RecordReaderBase<Key, Feature>() {
                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    if (scannerIterator.hasNext()) {
                        ++numKeysRead;
                        Map.Entry<Key, Value> entry = scannerIterator.next();
                        currentK = currentKey = entry.getKey();
                        currentV = entryTransform.apply(entry);

                        if (log.isTraceEnabled())
                            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
                        return true;
                    }
                    return false;
                }
            };
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
