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

import org.calrissian.accumulorecipes.rangestore.support.ValueRange;

public interface RangeHelper<T extends Comparable<T>> {

    /**
     * Simply verifies that the range is valid.
     *
     * @param range
     * @return
     */
    boolean isValid(ValueRange<T> range);

    /**
     * Returns the distance between the start and end elements.
     * <p/>
     * TODO.  The return value of this function is sort of a hack. This value needs
     * to be able to be compared and complemented, which this class allows.
     *
     * @param range
     * @return
     */
    T distance(ValueRange<T> range);

    /**
     * Encodes a value into a string that can be lexigraphically sorted.
     *
     * @param value
     * @return
     */
    String encode(T value);

    /**
     * Encodes the values in a string that can be lexigraphically sorted in reverse order.
     *
     * @param value
     * @return
     */
    String encodeComplement(T value);

    /**
     * Decodes the lexigraphical representation of the value.
     *
     * @param value
     * @return
     */
    T decode(String value);

    /**
     * Decodes the complements lexigraphical representation of the value.
     *
     * @param value
     * @return
     */
    T decodeComplement(String value);

}
