/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.TupleIndexKey;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.junit.Test;

public class CalculateShardsVisitorTest {


    @Test
    public void shardRollUpInAndNode() {
        Map<TupleIndexKey,Set<String>> shards = new HashMap<TupleIndexKey, Set<String>>();
        shards.put(new BaseCardinalityKey("key1", "val1", "string"), Sets.newHashSet("1", "2"));
        shards.put(new BaseCardinalityKey("key2", "val2", "string"), Sets.newHashSet("2", "3"));
        shards.put(new BaseCardinalityKey("key3", "val3", "string"), Sets.newHashSet("2", "5"));

        Node node = new QueryBuilder().and().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = new CalculateShardsVisitor(shards, LexiTypeEncoders.LEXI_TYPES);
        node.accept(visitor);

        System.out.println(visitor.getShards());

        assertEquals(1, visitor.getShards().size());
        assertEquals("2", visitor.getShards().iterator().next());
    }


    @Test
    public void shardRollUpInOrNode() {
        Map<TupleIndexKey,Set<String>> shards = new HashMap<TupleIndexKey, Set<String>>();
        shards.put(new BaseCardinalityKey("key1", "val1", "string"), Sets.newHashSet("1", "2"));
        shards.put(new BaseCardinalityKey("key2", "val2", "string"), Sets.newHashSet("2", "3"));
        shards.put(new BaseCardinalityKey("key3", "val3", "string"), Sets.newHashSet("2", "5"));

        Node node = new QueryBuilder().or().eq("key3", "val3").or().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = new CalculateShardsVisitor(shards, LexiTypeEncoders.LEXI_TYPES);
        node.accept(visitor);

        System.out.println(visitor.getShards());

        assertEquals(4, visitor.getShards().size());
    }

    @Test
    public void shardRollUpAlternatingAndOrNodes() {
        Map<TupleIndexKey,Set<String>> shards = new HashMap<TupleIndexKey, Set<String>>();
        shards.put(new BaseCardinalityKey("key1", "val1", "string"), Sets.newHashSet("1", "2"));
        shards.put(new BaseCardinalityKey("key2", "val2", "string"), Sets.newHashSet("2", "3"));
        shards.put(new BaseCardinalityKey("key3", "val3", "string"), Sets.newHashSet("2", "5"));

        Node node = new QueryBuilder().or().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = new CalculateShardsVisitor(shards, LexiTypeEncoders.LEXI_TYPES);
        node.accept(visitor);

        System.out.println(visitor.getShards());

        assertEquals(2, visitor.getShards().size());
    }

}
