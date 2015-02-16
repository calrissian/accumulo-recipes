package org.calrissian.accumulorecipes.commons.iterators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.swing.text.html.parser.Entity;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.calrissian.accumulorecipes.commons.util.RowEncoderUtil;

public abstract class MetadataExpirationFilter<T extends Entity> extends WrappingIterator {



    public boolean accept(Key k, Value v) {

        if(k.getColumnFamily().toString().startsWith(Constants.PREFIX_E)) {

            ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
            DataInputStream dis = new DataInputStream(bais);
            try {
                long expiration = dis.readLong();
                if(!shouldExpire(expiration, parseTimestampFromKey(k))) {

                    List<Map.Entry<Key,Value>> finalKeyValList = new ArrayList();
                    for(Map.Entry<Key,Value> curEntry : RowEncoderUtil.decodeRow(k, bais)) {
                        if(!shouldExpire(Long.parseLong(curEntry.getKey().getColumnFamily().toString()), curEntry.getKey().getTimestamp())) {
                            finalKeyValList.add(curEntry);
                        }
                    }


                }
            } catch (IOException e) {
                return true;
            }

        }
        else if(k.getColumnFamily().toString().startsWith(Constants.PREFIX_FI)) {
            long expiration = new GlobalIndexValue(v).getExpiration();
            return !shouldExpire(expiration, parseTimestampFromKey(k));
        }

        return true;
    }

    protected abstract T buildEntity(Key k, Value v);

    protected abstract long parseTimestampFromKey(Key k);


    private boolean shouldExpire(long expiration, long timestamp) {
        return (expiration > -1 && System.currentTimeMillis() - timestamp > expiration);
    }
}
