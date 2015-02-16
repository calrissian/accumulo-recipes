/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.iterators;

import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

/**
 * This iterator will propagate through the key rows of the event global index
 * table to return all the unique keys for the given set of rows.
 */
public class GlobalIndexUniqueKeyValueIterator extends FirstEntryInPrefixedRowIterator {

    @Override
    protected String getPrefix(String rowStr) {

        int idx = rowStr.lastIndexOf(NULL_BYTE);
        String substr = rowStr.substring(0, idx);
        return substr;
    }
}
