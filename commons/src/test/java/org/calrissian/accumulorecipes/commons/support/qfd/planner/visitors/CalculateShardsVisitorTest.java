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
package org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.calrissian.accumulorecipes.commons.support.qfd.TupleIndexKey;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.junit.Test;

public class CalculateShardsVisitorTest {

    private CalculateShardsVisitor runShardsVisitor(Node node) {

        Map<TupleIndexKey,Set<String>> shards = new HashMap<TupleIndexKey, Set<String>>();
        shards.put(new TupleIndexKey("key1", "val1", "string"), Sets.newHashSet("1", "2"));
        shards.put(new TupleIndexKey("key2", "val2", "string"), Sets.newHashSet("2", "3"));
        shards.put(new TupleIndexKey("key3", "val3", "string"), Sets.newHashSet("2", "5"));

        CalculateShardsVisitor visitor = new CalculateShardsVisitor(shards, LexiTypeEncoders.LEXI_TYPES);
        node.accept(visitor);

        System.out.println(visitor.getShards());

        return visitor;
    }


    @Test
    public void shardRollUpInAndNode() {

        /**
         * The result of an AND nested within an AND with an EQ as a sibling
         */
        Node node = new QueryBuilder().and().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(1, visitor.getShards().size());
        assertEquals("2", visitor.getShards().iterator().next());
    }


    @Test
    public void shardRollUpInOrNode() {

        /**
         * The result of nesting an OR within an OR with an EQ as a sibling
         */
        Node node = new QueryBuilder().or().eq("key3", "val3").or().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(4, visitor.getShards().size());
    }

    @Test
    public void shardRollUpAlternatingAndOrNodes() {

        /**
         * The result of nesting an AND within an OR with an EQ as a sibling
         */
        Node node = new QueryBuilder().or().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(2, visitor.getShards().size());
    }


    @Test
    public void shardRollUpAlternatingOrAndNodes() {

        /**
         * The result of nesting an OR within an AND with an EQ as a sibling
         */
        Node node = new QueryBuilder().and().eq("key3", "val3").or().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(1, visitor.getShards().size());
    }

    @Test
    public void shardRollUpAndNodesSomeDontExist() {

        /**
         * When we have a node that doesn't exist in an and, we shouldn't scan any shards
         */
        Node node = new QueryBuilder().and().eq("key4", "val2").eq("key2", "val2").end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(0, visitor.getShards().size());
    }


    @Test
    public void shardRollUpAndOrNodesSomeDontExist() {

        /**
         * When we have a node that doesn't exist in an and, the whole and should should fail but the or
         * should have shards
         */
        Node node = new QueryBuilder().or().eq("key1", "val1").and().eq("key4", "val2").eq("key2", "val2")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(2, visitor.getShards().size());
    }

    @Test
    public void shardRollUpOrNodesNegationNodesInAnd() {

        /**
         * When we have a negation node and we OR that with another node.
         */
        Node node = new QueryBuilder().or().eq("key1", "val1").and().notEq("key2", "val2")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(3, visitor.getShards().size());
    }


    @Test
    public void shardRollUpAndNodesNegationNodesInOr() {

        /**
         * This pretty much becomes an and. The only shard in common is 2
         */
        Node node = new QueryBuilder().and().eq("key1", "val1").or().notEq("key2", "val2")
            .end().end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(1, visitor.getShards().size());
    }


    @Test
    public void test() {

        /**
         * The result of nesting an OR within an AND with an EQ as a sibling
         */
        Node node = new QueryBuilder().and().greaterThan("key1", "val1").lessThan("key2", "val2").notEq("key1", "hellp")
            .end().build();

        CalculateShardsVisitor visitor = runShardsVisitor(node);

        assertEquals(1, visitor.getShards().size());
    }



}
