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
package org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors;

import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.util.Collections;

import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.QueryPlanner;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.junit.Test;

public class QueryOptimizerTest {

    @Test
    public void test() {

        Node query = QueryBuilder.create().and().and().or().end().end().end().build();

        QueryPlanner optimizer = new QueryPlanner(query, LEXI_TYPES);

        System.out.println(new NodeToJexl(LEXI_TYPES).transform(Collections.singleton(""), optimizer.getOptimizedQuery()));

    }
}
