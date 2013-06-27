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

import static java.lang.Long.parseLong;

public class LongRangeHelper implements RangeHelper<Long> {

    //private static final LongNormalizer normalizer = new LongNormalizer();

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

        //TODO: Use math to solve this problem.
        //TODO: The problem is the default normalizer doesn't normalize negative numbers lexigraphically.
        String prefix;
        if (value >= 0) {
            prefix = "0";
        } else {
            prefix = "-";
            value = Long.MAX_VALUE + value;
        }

        return prefix + String.format("%020d", value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encodeComplement(Long value) {
        return encode(Long.MAX_VALUE - value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long decode(String value) {

        //TODO: Use math to solve this problem.
        //TODO: The problem is the default normalizer doesn't normalize negative numbers lexigraphically.
        if (value.startsWith("-"))
            return -1 * (Long.MAX_VALUE - parseLong(value.substring(1)));
        else
            return parseLong(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long decodeComplement(String value) {
        return Long.MAX_VALUE - decode(value);
    }
}
