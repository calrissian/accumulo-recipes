package org.calrissian.accumulorecipes.featurestore.hadoop;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.featurestore.feature.BaseFeature;
import org.calrissian.accumulorecipes.featurestore.feature.transform.AccumuloFeatureConfig;
import org.calrissian.accumulorecipes.featurestore.support.FeatureEntryTransform;
import org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore.DEFAULT_TABLE_NAME;
import static org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore.combine;
import static org.calrissian.accumulorecipes.featurestore.support.Constants.DEFAULT_ITERATOR_PRIORITY;
import static org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry.BASE_FEATURES;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;

public class FeaturesInputFormat extends InputFormatBase<Key, BaseFeature> {

    public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
        setInputInfo(config, username, password, DEFAULT_TABLE_NAME, auths);
    }

    public static void setQueryInfo(Configuration config, Date start, Date end, MetricTimeUnit timeUnit, String group, String type, String name, Class<? extends BaseFeature> featureType) throws IOException {
        setQueryInfo(config, start, end, timeUnit, group, type, name, featureType, BASE_FEATURES);
    }


    public static void setQueryInfo(Configuration config, Date start, Date end, MetricTimeUnit timeUnit, String group, String type, String name, Class<? extends BaseFeature> featureType, FeatureRegistry registry) throws IOException {

        AccumuloFeatureConfig featureConfig = registry.transformForClass(featureType);

        config.set("featureConfig", new String(toBase64(featureConfig)));
        config.set("timeUnit", timeUnit.toString());

        setRanges(config,
            singleton(new Range(
                combine(group, generateTimestamp(end.getTime(), timeUnit)),
                combine(group, generateTimestamp(start.getTime(), timeUnit))
            ))
        );

        if (name != null) {
            Pair<Text, Text> column = new Pair<Text, Text>(new Text(combine(featureConfig.featureName(), timeUnit.toString())), new Text(combine(type, name)));
            fetchColumns(config, singleton(column));
        } else {
            Pair<Text, Text> column = new Pair<Text, Text>(new Text(combine(featureConfig.featureName(), timeUnit.toString())), null);
            fetchColumns(config, singleton(column));

            String cqRegex = null;
            cqRegex = combine(type, "(.*)");
            IteratorSetting regexIterator = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY - 1, "regex", RegExFilter.class);
            RegExFilter.setRegexs(regexIterator, null, null, cqRegex, null, false);
            addIterator(config, regexIterator);
        }
    }

    @Override
    public RecordReader<Key, BaseFeature> createRecordReader(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {


        MetricTimeUnit timeUnit = MetricTimeUnit.valueOf(context.getConfiguration().get("timeUnit"));

        try {
            final AccumuloFeatureConfig<? extends BaseFeature> config = fromBase64(context.getConfiguration().get("featureConfig").getBytes());
            final FeatureEntryTransform<? extends BaseFeature> entryTransform = new FeatureEntryTransform<BaseFeature>(timeUnit) {
                @Override
                protected BaseFeature transform(long timestamp, String group, String type, String name, String visibility, Value value) {
                    return config.buildFeatureFromValue(timestamp, type, group, name, visibility, value);
                }
            };

            return new RecordReaderBase<Key, BaseFeature>() {
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
