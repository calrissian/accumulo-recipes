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
import org.calrissian.mango.types.exception.TypeEncodingException;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.eventstore.support.Constants.SHARD_PREFIX_B;


public class AndNodeColumns {

    protected  final Text[] columns;
    protected  final boolean[] notFlags;

    private static Text generateColumn(String key, Object value, TypeRegistry<String> typeRegistry) {
        try {
            return new Text(SHARD_PREFIX_B + DELIM + key + DELIM + typeRegistry.getAlias(value)
                    + DELIM + typeRegistry.encode(value));
        } catch (TypeEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public AndNodeColumns(AndNode query, TypeRegistry<String> typeRegistry) {

        this.columns = new Text[query.children().size()];
        this.notFlags = new boolean[columns.length];

        int i = 0;
        for (Node node : query.children()) {
            if (node instanceof NotEqualsLeaf) {
                this.notFlags[i] = true;
                NotEqualsLeaf leaf = (NotEqualsLeaf) node;
                this.columns[i] = generateColumn(leaf.getKey(), leaf.getValue(), typeRegistry);
            } else if (node instanceof EqualsLeaf) {
                EqualsLeaf leaf = (EqualsLeaf) node;
                this.columns[i] = generateColumn(leaf.getKey(), leaf.getValue(), typeRegistry);
            }
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
