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
package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;

import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;

public class EntityCardinalityKey extends BaseCardinalityKey {

    public EntityCardinalityKey(Key key) {

        String row = key.getRow().toString();
        if (row.indexOf("_" + INDEX_V + "_") != -1) {
            this.alias = row.substring(row.indexOf("_" + INDEX_V + "_") + 3, row.indexOf("__"));
            this.normalizedValue = row.substring(row.indexOf("__") + 2, row.length());
            this.key = key.getColumnFamily().toString();
        } else if (row.indexOf("_" + INDEX_K + "_") != -1) {
            this.key = row.substring(row.indexOf("_" + INDEX_K + "_") + 3, row.length());
            this.alias = key.getColumnFamily().toString();
        }
    }
}
