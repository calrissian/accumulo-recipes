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
package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CardinalityReorderVisitorTest {

    @Test
    public void test_basicReorder() {

        Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();
        cardinalities.put(new BaseCardinalityKey("key1", "val1", "string"), 500l);
        cardinalities.put(new BaseCardinalityKey("key2", "val2", "string"), 50l);
        cardinalities.put(new BaseCardinalityKey("key3", "val3", "string"), 1000l);

        Node node = new QueryBuilder().or().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
                .end().end().build();

        node.accept(new CardinalityReorderVisitor(cardinalities));

        System.out.println(new NodeToJexl().transform(node));

        assertTrue(node instanceof OrNode);
        assertTrue(node.children().get(0) instanceof AndNode);
        assertEquals("key2", ((AbstractKeyValueLeaf) node.children().get(0).children().get(0)).getKey());
        assertEquals("key1", ((AbstractKeyValueLeaf) node.children().get(0).children().get(1)).getKey());
        assertEquals("key3", ((AbstractKeyValueLeaf) node.children().get(1)).getKey());
    }

    @Test
    public void test_pruneCardinalities_AndNode() {

        Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();
        cardinalities.put(new BaseCardinalityKey("key1", "val1", "string"), 500l);
        cardinalities.put(new BaseCardinalityKey("key2", "val2", "string"), 0l);
        cardinalities.put(new BaseCardinalityKey("key3", "val3", "string"), 1000l);

        Node node = new QueryBuilder().or().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
                .end().end().build();

        node.accept(new CardinalityReorderVisitor(cardinalities));

        System.out.println(new NodeToJexl().transform(node));

        assertTrue(node instanceof OrNode);
        assertTrue(node.children().get(0) instanceof EqualsLeaf);
        assertEquals(1, node.children().size());
        assertEquals("key3", ((AbstractKeyValueLeaf) node.children().get(0)).getKey());
    }

    @Test
    public void test_pruneCardinalities_OrNode() {

        Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();
        cardinalities.put(new BaseCardinalityKey("key1", "val1", "string"), 0l);
        cardinalities.put(new BaseCardinalityKey("key2", "val2", "string"), 0l);
        cardinalities.put(new BaseCardinalityKey("key3", "val3", "string"), 1000l);

        Node node = new QueryBuilder().or().eq("key3", "val3").or().eq("key2", "val2").eq("key1", "val1")
                .end().end().build();

        node.accept(new CardinalityReorderVisitor(cardinalities));

        System.out.println(new NodeToJexl().transform(node));

        assertTrue(node instanceof OrNode);
        assertTrue(node.children().get(0) instanceof EqualsLeaf);
        assertEquals(1, node.children().size());
        assertEquals("key3", ((AbstractKeyValueLeaf) node.children().get(0)).getKey());
    }

    @Test
    public void test_pruneCardinalities_AllNodesZero() {

        Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();
        cardinalities.put(new BaseCardinalityKey("key1", "val1", "string"), 0l);
        cardinalities.put(new BaseCardinalityKey("key2", "val2", "string"), 0l);
        cardinalities.put(new BaseCardinalityKey("key3", "val3", "string"), 0l);

        Node node = new QueryBuilder().or().eq("key3", "val3").or().eq("key2", "val2").eq("key1", "val1")
                .end().end().build();

        node.accept(new CardinalityReorderVisitor(cardinalities));

        System.out.println(new NodeToJexl().transform(node));

        assertTrue(node instanceof OrNode);
        assertEquals(0, node.children().size());
    }


}
