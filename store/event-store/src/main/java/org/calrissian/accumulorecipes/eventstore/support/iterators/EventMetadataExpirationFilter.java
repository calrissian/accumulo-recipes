package org.calrissian.accumulorecipes.eventstore.support.iterators;

import java.io.IOException;
import java.util.HashMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;
import org.calrissian.accumulorecipes.commons.support.Constants;

public class EventMetadataExpirationFilter extends MetadataExpirationFilter{
    @Override
    protected long parseTimestampFromKey(Key k) {

        String cf = k.getColumnFamily().toString();
        int idx = cf.lastIndexOf(Constants.ONE_BYTE);
        return Long.parseLong(cf.substring(idx+1, cf.length()));
    }


    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EventMetadataExpirationFilter ret = new EventMetadataExpirationFilter();
        try {
            ret.init(getSource(), new HashMap<String,String>(), env);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
