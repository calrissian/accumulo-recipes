package org.calrissian.accumulorecipes.eventstore.support;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;
import org.calrissian.accumulorecipes.commons.support.Constants;

public class EventTimeLimitingFilter extends TimeLimitingFilter {

    @Override
    protected long parseTimestamp(Key k, Value v) {

        if(k.getColumnFamily().toString().startsWith(Constants.PREFIX_FI))
            return k.getTimestamp();

        ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
        DataInputStream dis = new DataInputStream(bais);

        long ret = -1;
        try {
            dis.readLong();
            ret = dis.readLong();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return ret;
    }
}
