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

import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;

import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

public class EventCardinalityKey extends BaseCardinalityKey {

    public EventCardinalityKey(Key key) {

        String row = key.getRow().toString();
        int part0Idx = row.indexOf("_");
        int part1Idx = row.indexOf("__");
        int firstNBIdx = row.indexOf(NULL_BYTE);

        if (row.startsWith(INDEX_V)) {
            int lastNBIdx = row.lastIndexOf(NULL_BYTE);
            this.alias = row.substring(part0Idx + 1, part1Idx);
            this.key = row.substring(part1Idx+2, firstNBIdx);
            this.normalizedValue = row.substring(firstNBIdx + 1, lastNBIdx);
            this.shard = row.substring(lastNBIdx+1, row.length());
        } else if (row.startsWith(INDEX_K)) {
          this.key = row.substring(part0Idx+1, part1Idx);
          this.alias = row.substring(part1Idx+2, firstNBIdx);
          this.shard = row.substring(firstNBIdx + 1, row.length());
        }
    }
}
