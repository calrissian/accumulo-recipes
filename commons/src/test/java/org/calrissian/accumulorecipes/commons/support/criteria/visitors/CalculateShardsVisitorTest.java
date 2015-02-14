package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.junit.Test;

public class CalculateShardsVisitorTest {


    @Test
    public void shardRollUpInAndNode() {
        Map<CardinalityKey,Set<String>> shards = new HashMap<CardinalityKey, Set<String>>();
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
        Map<CardinalityKey,Set<String>> shards = new HashMap<CardinalityKey, Set<String>>();
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
        Map<CardinalityKey,Set<String>> shards = new HashMap<CardinalityKey, Set<String>>();
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
