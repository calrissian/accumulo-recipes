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
package org.calrissian.accumulorecipes.rangestore.helper;

import org.calrissian.mango.domain.ValueRange;

import static java.lang.Character.digit;

public class LongRangeHelper implements RangeHelper<Long> {

    //TODO move this logic to mango when the types situation gets worked out.
    private static final char POS = '0';
    private static final char NEG = '-';

    /**
     * Helper function simply because Long.parseLong(hex,16) does not handle negative numbers that were
     * converted to hex.
     */
    private static long fromHex(String hex) {
        long value = 0;
        for (int i = 0; i < hex.length(); i++)
            value = (value << 4) | digit(hex.charAt(i), 16);

        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(ValueRange<Long> range) {
        return range.getStop() >= range.getStart();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long distance(ValueRange<Long> range) {
        return range.getStop() - range.getStart();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encode(Long value) {
        //First have neg and positive characters simply to allow positives to sort first
        //The second part works because java represents negative numbers as 2's compliment
        //Then when it converts to hex in the format, it simply encodes the bits.
        //This property means that negative numbers when converted to hex are already lexicographically sorted.
        return String.format("%c%016x", (value < 0 ? NEG : POS), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encodeComplement(Long value) {
        return encode(~value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long decode(String value) {
        //Ignore first character and convert from hex back to a long.
        return fromHex(value.substring(1));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long decodeComplement(String value) {
        return ~decode(value);
    }
}
