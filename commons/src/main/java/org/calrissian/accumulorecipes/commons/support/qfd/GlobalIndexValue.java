package org.calrissian.accumulorecipes.commons.support.qfd;

import org.apache.accumulo.core.data.Value;

public class GlobalIndexValue {

    private final long cardinatlity;
    private final long expiration;

    public GlobalIndexValue(Value value) {

        String str = new String(value.get());

        int idx = str.indexOf(",");

        if(idx == -1) {
            cardinatlity = Long.parseLong(str);
            expiration = -1;
        } else {
            cardinatlity = Long.parseLong(str.substring(0, idx));
            expiration = Long.parseLong(str.substring(idx + 1, str.length()));
        }
    }

    public GlobalIndexValue(long cardinality, long expiration) {

        this.cardinatlity = cardinality;
        this.expiration = expiration;
    }

    public long getCardinatlity() {
        return cardinatlity;
    }

    public long getExpiration() {
        return expiration;
    }

    public Value toValue() {
        return new Value((Long.toString(cardinatlity) + "," + Long.toString(expiration)).getBytes());
    }
}
