package org.calrissian.accumlorecipes.changelog.support;

import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.mango.hash.support.HashUtils;
import org.calrissian.mango.types.TypeContext;
import org.calrissian.mango.types.exception.TypeNormalizationException;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.calrissian.accumlorecipes.changelog.support.Constants.DELIM;

public class Utils {

    static TypeContext context = TypeContext.getInstance();

    public static final Long MAX_MINUTE = 999999999999l;
    public static final Long MAX_SECOND = 99999999999999l;
    public static final String MINUTE_FORMAT = "yyyyMMddHHmm";
    public static final String SECOND_FORMAT = "yyyyMMddHHmmss";


    /**
     * Tuples are hashed by sorting them by their keys, normalized values, and visibilities.
     * @param entry
     * @return
     */
    public static byte[] hashEntry(StoreEntry entry) {

        Comparator c = new TupleComparator();
        List<Tuple> tuples = new ArrayList(entry.getTuples());

        Collections.sort(tuples, c);

        String tupleString = entry.getId();
        for(Tuple tuple : tuples) {
            tupleString += Utils.tupleToString(tuple) + ",";
        }

        try {
            return HashUtils.hashString(tupleString).getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static Long truncatedReverseTimestamp(long timestamp, TimeUnit timeUnit) {
        String minutes = new SimpleDateFormat(MINUTE_FORMAT).format(new Date(timestamp));
        Long l = Long.parseLong(minutes);
        long revTs = MAX_MINUTE - l;
        switch (timeUnit) {
            case MINUTES:
                return revTs;
            case HOURS:
                return revTs / 100;
            case DAYS:
                return revTs / 10000;
        }

        throw new IllegalArgumentException("Unsupported time unit");
    }

    public static Long reverseTimestamp(long timestamp) {
        String seconds = new SimpleDateFormat(SECOND_FORMAT).format(new Date(timestamp));
        Long l = Long.parseLong(seconds);
        long revTs = MAX_SECOND - l;

        return revTs;
    }



    public static String tupleToString(Tuple tuple) {

        try {
            return tuple.getKey() + DELIM + context.normalize(tuple.getValue()) +
                    "\u0000" + tuple.getVisibility();
        } catch (TypeNormalizationException e) {
            throw new RuntimeException(e);
        }
    }


}
