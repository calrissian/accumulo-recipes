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
package org.calrissian.accumulorecipes.commons.iterators.support;

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NodeToJexlTest {

    private NodeToJexl nodeToJexl = new NodeToJexl();

    @Test
    public void testSimpleEquals_AndNode() {
        String jexl = nodeToJexl.transform(new QueryBuilder().and().eq("hello", "goodbye").eq("key1", true).end().build());
        assertEquals("((hello == 'string\u0001goodbye') and (key1 == 'boolean\u00011'))", jexl);
    }

    @Test
    public void testSimpleEquals_OrNode() {
        String jexl = nodeToJexl.transform(new QueryBuilder().or().eq("hello", "goodbye").eq("key1", true).end().build());
        assertEquals("((hello == 'string\u0001goodbye') or (key1 == 'boolean\u00011'))", jexl);
    }

    @Test
    public void testGreaterThan() {
        String jexl = nodeToJexl.transform(new QueryBuilder().greaterThan("hello", "goodbye").build());
        assertEquals("((hello > 'string\u0001goodbye'))", jexl);
    }

    @Test
    public void testLessThan() {
        String jexl = nodeToJexl.transform(new QueryBuilder().lessThan("hello", "goodbye").build());
        assertEquals("((hello < 'string\u0001goodbye'))", jexl);
    }


    @Test
    public void testGreaterThanEquals() {
        String jexl = nodeToJexl.transform(new QueryBuilder().greaterThanEq("hello", "goodbye").build());
        assertEquals("((hello >= 'string\u0001goodbye'))", jexl);
    }

    @Test
    public void testLessThanEquals() {
        String jexl = nodeToJexl.transform(new QueryBuilder().lessThanEq("hello", "goodbye").build());
        assertEquals("((hello <= 'string\u0001goodbye'))", jexl);
    }

    @Test
    public void testNotEquals() {
        String jexl = nodeToJexl.transform(new QueryBuilder().notEq("hello", "goodbye").build());
        assertEquals("((hello != 'string\u0001goodbye'))", jexl);
    }

    @Test
    public void testHas() {
        String jexl = nodeToJexl.transform(new QueryBuilder().has("hello").build());
        assertEquals("((hello >= '\u0000'))", jexl);
    }

    @Test
    public void testHasNot() {
        String jexl = nodeToJexl.transform(new QueryBuilder().hasNot("hello").build());
        assertEquals("(!(hello >= '\u0000'))", jexl);
    }

}

