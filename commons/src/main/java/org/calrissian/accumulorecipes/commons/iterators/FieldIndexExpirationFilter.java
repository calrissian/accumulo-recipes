package org.calrissian.accumulorecipes.commons.iterators;

import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_FI;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public class FieldIndexExpirationFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
        String cf = k.getColumnFamily().toString();
        String row = k.getRow().toString();
        if(cf.startsWith(PREFIX_FI)) {
            long expiration = Long.parseLong(new String(v.get()));
            return !MetadataExpirationFilter.shouldExpire(expiration, k.getTimestamp());
        }

        return true;

    }
}
