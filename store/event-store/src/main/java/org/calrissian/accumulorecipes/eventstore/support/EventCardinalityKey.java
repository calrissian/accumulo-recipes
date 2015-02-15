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
package org.calrissian.accumulorecipes.eventstore.support;

import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.eventstore.support.EventKeyValueIndex.INDEX_SEP;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.StringUtils;
import org.calrissian.accumulorecipes.commons.support.qfd.TupleIndexKey;

public class EventCardinalityKey extends TupleIndexKey {



    public EventCardinalityKey(Key key) {

        String row = key.getRow().toString();
        String parts[] = StringUtils.splitByWholeSeparatorPreserveAllTokens(row, INDEX_SEP);
        int firstNBIdx = parts[3].indexOf(NULL_BYTE);

        if (row.startsWith(INDEX_V)) {
            int lastNBIdx = parts[3].lastIndexOf(NULL_BYTE);
            this.alias = parts[2];
            this.key = parts[3].substring(0, firstNBIdx);
            this.normalizedValue = parts[3].substring(firstNBIdx + 1, lastNBIdx);
            this.shard = parts[3].substring(lastNBIdx + 1, parts[3].length());
        } else if (row.startsWith(INDEX_K)) {
            this.key = parts[2];
            this.alias = parts[3].substring(0, firstNBIdx);
            this.shard = parts[3].substring(firstNBIdx + 1, parts[3].length());
        }
    }
}
