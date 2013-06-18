package org.calrissian.accumulorecipes.metricsstore.support;

import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;

import static java.util.Map.Entry;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.metricsstore.support.TimestampUtil.revertTimestamp;

/**
 * Simple utility class to extract the metadata from a Key/Value to allow specific Metric types to be returned.
 * @param <T>
 */
public abstract class MetricTransform<T> implements Function<Entry<Key, Value>, T> {

    MetricTimeUnit timeUnit;

    public MetricTransform(MetricTimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    protected abstract T transform(long timestamp, String group, String type, String name, String visibility, Value value);

    @Override
    public T apply(Entry<Key, Value> entry) {

        String row[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), DELIM);
        String colQ[] = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), DELIM);

        return transform(
                revertTimestamp(row[1], timeUnit),
                row[0],
                colQ[0],
                colQ[1],
                entry.getKey().getColumnVisibility().toString(),
                entry.getValue()
        );

    }
}
