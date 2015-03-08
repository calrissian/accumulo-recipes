/*
* Copyright (C) 2014 The Calrissian Authors
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

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractInNotInVisitorTest {

    @Test
    public void testIn() {

        Node query = QueryBuilder.create().in("key", "hello", "goodbye").build();
        Node expected = QueryBuilder.create().and().or().eq("key", "hello").eq("key", "goodbye").end().end().build();
        query.accept(new ExtractInNotInVisitor());

        assertEquals(expected, query);
    }
}
