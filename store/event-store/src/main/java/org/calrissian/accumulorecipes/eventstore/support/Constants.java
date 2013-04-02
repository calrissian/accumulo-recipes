package org.calrissian.accumulorecipes.eventstore.support;


public class Constants {

    public static final int DEFAULT_PARTITION_SIZE = 15;

    public static final String SHARD_PREFIX_B = "b";    // backwards index (key/value:uuid)
    public static final String SHARD_PREFIX_F = "f";    // forwards index (uuid:key/value)
    public static final String SHARD_PREFIX_V = "v";    // value index    (value:key/uuid)

    public static final String DELIM = "\u0000";
    public static final String DELIM_END = "\uffff";

}
