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


import org.calrissian.mango.types.exception.TypeNormalizationException;
import org.calrissian.mango.types.normalizers.LongNormalizer;
import org.calrissian.mango.types.range.ValueRange;

public class LongRangeHelper implements RangeHelper<Long> {

    private static final LongNormalizer normalizer = new LongNormalizer();

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
        try {
            return normalizer.normalize(value);
        } catch (TypeNormalizationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encodeComplement(Long value) {
        try {
            return normalizer.normalize(Long.MAX_VALUE - value);
        } catch (TypeNormalizationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long decode(String value) {
        try {
            return normalizer.denormalize(value);
        } catch (TypeNormalizationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long decodeComplement(String value) {
        try {
            return Long.MAX_VALUE - normalizer.denormalize(value);
        } catch (TypeNormalizationException e) {
            throw new RuntimeException(e);
        }
    }
}
