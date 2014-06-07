/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumlorecipes.changelog.support;

import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Long.parseLong;
import static java.util.Collections.sort;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.calrissian.accumlorecipes.changelog.support.Constants.DELIM;

public class Utils {

    public static final Long MAX_TIME = 999999999999999999l;
    public static final String DATE_FORMAT = "yyyyMMddHHmmssSSS";

    /**
     * Tuples are hashed by sorting them by their keys, normalized values, and visibilities.
     *
     * @param entry
     * @return
     */
    public static byte[] hashEntry(Event entry, TypeRegistry<String> typeRegistry) {

        List<Tuple> tuples = new ArrayList(entry.getTuples());

        sort(tuples, new TupleComparator(typeRegistry));

        String tupleString = entry.getId();
        for (Tuple tuple : tuples)
            tupleString += tupleToString(tuple, typeRegistry) + ",";

        try {
            return md5Hex(tupleString).getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Defaults to 15 mins.
     *
     * @param timestamp
     * @return
     */
    public static Long truncatedReverseTimestamp(long timestamp, BucketSize bucketSize) {
        timestamp = timestamp - (timestamp % bucketSize.getMs());
        String minutes = new SimpleDateFormat(DATE_FORMAT).format(new Date(timestamp));

        return MAX_TIME - parseLong(minutes);
    }

    public static Long reverseTimestampToNormalTime(long timestamp) {

        Long convert = MAX_TIME - timestamp;
        try {
            return new SimpleDateFormat(DATE_FORMAT).parse(Long.toString(convert)).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Long reverseTimestamp(long timestamp) {
        String seconds = new SimpleDateFormat(DATE_FORMAT).format(new Date(timestamp));
        long revTs = MAX_TIME - parseLong(seconds);

        return revTs;
    }

    public static String tupleToString(Tuple tuple, TypeRegistry<String> typeRegistry) {
        return tuple.getKey() + DELIM + typeRegistry.encode(tuple.getValue()) + DELIM + tuple.getMetadata();
    }
}
