package org.calrissian.accumlorecipes.changelog.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.support.BucketSize;
import org.calrissian.accumlorecipes.changelog.support.Utils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class BucketRollupIterator extends WrappingIterator {

    protected BucketSize bucketSize = BucketSize.FIVE_MINS;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);    //To change body of overridden methods use File | Settings | File Templates.

        if(options.containsKey("bucketSize")) {
            bucketSize = BucketSize.valueOf(options.get("bucketSize"));
        }
    }

    @Override
    public boolean hasTop() {
        return super.hasTop();
    }

    @Override
    public void next() throws IOException {
        super.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Key getTopKey() {
        Key topKey = super.getTopKey();

        long timestamp = Utils.reverseTimestampToNormalTime(Long.parseLong(topKey.getRow().toString()));

        Key retKey =  new Key(new Text(Utils.truncatedReverseTimestamp(timestamp, bucketSize).toString()),
                       topKey.getColumnFamily(), topKey.getColumnQualifier(),
                       new Text(topKey.getColumnVisibility().toString()), topKey.getTimestamp());

        return retKey;
    }

    @Override
    public Value getTopValue() {
        return super.getTopValue();
    }

    public static void setBucketSize(IteratorSetting is, BucketSize bucketSize) {

        is.addOption("bucketSize", bucketSize.name());
    }
}
