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

import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.Entity;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

public class EntityShardBuilder implements ShardBuilder<Entity> {

    private int partitionSize;

    public EntityShardBuilder(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    public String buildShard(String entityType, String entityId) {
        int partition = Math.abs(entityId.hashCode() % partitionSize);
        return buildShard(entityType, partition);
    }

    public String buildShard(String entityType, int partition) {
        return format("%s_%" + Integer.toString(partitionSize).length() + "d", entityType, partition);
    }

    public Set<Text> buildShardsForTypes(Collection<String> types) {
        Set<Text> ret = new HashSet<Text>();
        for (int i = 0; i < partitionSize; i++) {
            for (String type : types)
                ret.add(new Text(buildShard(type, i)));
        }
        return ret;
    }

    @Override
    public String buildShard(Entity item) {
        return buildShard(item.getType(), item.getId());
    }
}
