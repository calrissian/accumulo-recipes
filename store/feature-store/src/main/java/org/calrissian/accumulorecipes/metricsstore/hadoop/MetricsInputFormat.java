package org.calrissian.accumulorecipes.metricsstore.hadoop;

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
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.support.MetricTransform;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloFeatureStore.DEFAULT_TABLE_NAME;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloFeatureStore.combine;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DEFAULT_ITERATOR_PRIORITY;

public class MetricsInputFormat extends InputFormatBase<Key, FeatureWritable> {

    public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
        setInputInfo(config, username, password, DEFAULT_TABLE_NAME, auths);
    }

    public static void setQueryInfo(Configuration config, Date start, Date end, MetricTimeUnit timeUnit, String group, String type, String name) {

        config.set("timeUnit", timeUnit.toString());

        setRanges(config,
                singleton(new Range(
                        combine(group, generateTimestamp(end.getTime(), timeUnit)),
                        combine(group, generateTimestamp(start.getTime(), timeUnit))
                ))
        );

        if (name != null) {
            Pair<Text, Text> column = new Pair<Text, Text>(new Text(timeUnit.toString()), new Text(combine(type, name)));
            fetchColumns(config, singleton(column));
        } else {
            Pair<Text, Text> column = new Pair<Text, Text>(new Text(timeUnit.toString()), null);
            fetchColumns(config, singleton(column));

            String cqRegex = null;
            cqRegex = combine(type, "(.*)");
            IteratorSetting regexIterator = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY - 1, "regex", RegExFilter.class);
            RegExFilter.setRegexs(regexIterator, null, null, cqRegex, null, false);
            addIterator(config, regexIterator);
        }
    }

    @Override
    public RecordReader<Key, FeatureWritable> createRecordReader(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {

        final MetricTransform<Metric> xform = new MetricTransform<Metric>(MetricTimeUnit.valueOf(context.getConfiguration().get("timeUnit"))) {
            @Override
            protected Metric transform(long timestamp, String type, String group, String name, String visibility, Value value) {
                return new Metric(timestamp, group, type, name, visibility, Long.parseLong(value.toString()));
            }
        };

        final FeatureWritable sharedWritable = new FeatureWritable();

        return new RecordReaderBase<Key, FeatureWritable>() {
            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (scannerIterator.hasNext()) {
                    ++numKeysRead;
                    Map.Entry<Key, Value> entry = scannerIterator.next();
                    currentK = currentKey = entry.getKey();
                    sharedWritable.set(xform.apply(entry));
                    currentV = sharedWritable;

                    if (log.isTraceEnabled())
                        log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
                    return true;
                }
                return false;
            }
        };
    }
}
