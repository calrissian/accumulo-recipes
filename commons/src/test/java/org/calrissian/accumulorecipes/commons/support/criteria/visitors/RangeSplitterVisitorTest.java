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

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeSplitterVisitorTest {

    @Test
    public void test() {

        Node query = new QueryBuilder().and().eq("key1", "val1").range("key2", 0, 5).end().build();
        query.accept(new RangeSplitterVisitor());

        assertTrue(query instanceof AndNode);
        assertEquals(3, query.children().size());
        assertTrue(query.children().get(0) instanceof EqualsLeaf);
        assertTrue(query.children().get(1) instanceof GreaterThanEqualsLeaf);
        assertTrue(query.children().get(2) instanceof LessThanEqualsLeaf);

        query = new QueryBuilder().or().eq("key1", "val1").range("key2", 0, 5).end().build();
        query.accept(new RangeSplitterVisitor());

        assertTrue(query instanceof OrNode);
        assertEquals(2, query.children().size());
        assertTrue(query.children().get(0) instanceof EqualsLeaf);
        assertTrue(query.children().get(1) instanceof AndNode);
        assertTrue(query.children().get(1).children().get(0) instanceof GreaterThanEqualsLeaf);
        assertTrue(query.children().get(1).children().get(1) instanceof LessThanEqualsLeaf);

    }

}
