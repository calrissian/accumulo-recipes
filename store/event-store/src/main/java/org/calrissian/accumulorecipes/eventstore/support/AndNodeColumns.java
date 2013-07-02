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

import org.apache.hadoop.io.Text;
import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.EqualsLeaf;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.types.TypeRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.eventstore.support.Constants.SHARD_PREFIX_B;


public class AndNodeColumns {

    protected  final Text[] columns;
    protected  final boolean[] notFlags;

    public AndNodeColumns(AndNode query, TypeRegistry<String> typeRegistry) {

        Map<String, Object> fields = new HashMap<String, Object>();
        List<String> notFields = new ArrayList<String>();
        for (Node node : query.children()) {
            if (node instanceof NotEqualsLeaf) {
                NotEqualsLeaf notEqualsLeaf = (NotEqualsLeaf) node;
                notFields.add(notEqualsLeaf.getKey());
                fields.put(notEqualsLeaf.getKey(), notEqualsLeaf.getValue());
            } else if (node instanceof EqualsLeaf) {
                EqualsLeaf equalsLeaf = (EqualsLeaf) node;
                fields.put(equalsLeaf.getKey(), equalsLeaf.getValue());
            }
        }

        this.columns = new Text[fields.size()];
        this.notFlags = new boolean[fields.size()];

        int i = 0;
        for(Map.Entry<String, Object> entry : fields.entrySet()) {

            this.columns[i] = new Text(SHARD_PREFIX_B + DELIM + entry.getKey() + DELIM + typeRegistry.getAlias(entry.getValue())
                    + DELIM + entry.getValue());

            this.notFlags[i] = notFields.contains(entry.getKey()) ? true : false;
            i++;
        }
    }

    public Text[] getColumns() {
        return columns;
    }

    public boolean[] getNotFlags() {
        return notFlags;
    }
}
