package org.calrissian.accumulorecipes.commons.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.commons.util.RowEncoderUtil;

public abstract class MetadataExpirationFilter extends WrappingIterator {

    public Value extractExpiredTuples(Key k, Value v) {

        if(k.getColumnFamily().toString().startsWith(Constants.PREFIX_E)) {

            ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
            DataInputStream dis = new DataInputStream(bais);
            try {
                int size = dis.readInt();
                long expiration = dis.readLong();
                if(shouldExpire(expiration, parseTimestampFromKey(k))) {

                    long newMinExpiration = Long.MAX_VALUE;
                    List<Map.Entry<Key,Value>> finalKeyValList = new ArrayList();
                    for(Map.Entry<Key,Value> curEntry : RowEncoderUtil.decodeRow(k, bais)) {
                        long curExpiration = Long.parseLong(curEntry.getKey().getColumnFamily().toString());
                        if(!shouldExpire(curExpiration, curEntry.getKey().getTimestamp())) {
                            Math.min(curExpiration, newMinExpiration);
                            finalKeyValList.add(curEntry);
                        }
                    }

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    dos.writeInt(finalKeyValList.size());
                    dos.writeLong(newMinExpiration != Long.MAX_VALUE ? newMinExpiration : -1);
                    dos.flush();
                    RowEncoderUtil.encodeRow(finalKeyValList, baos);
                    baos.flush();

                    return new Value(baos.toByteArray());
                }
            } catch (IOException e) {
                return v;
            }
        }

        return v;
    }

    protected abstract long parseTimestampFromKey(Key k);


    public static boolean shouldExpire(long expiration, long timestamp) {
        return (expiration > -1 && System.currentTimeMillis() - timestamp > expiration);
    }

    @Override
    public Value getTopValue() {
        // apply expiration
        return extractExpiredTuples(getTopKey(), super.getTopValue());
    }

}

