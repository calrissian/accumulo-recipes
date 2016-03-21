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

import org.calrissian.accumulorecipes.commons.support.qfd.AttributeIndexKey;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Map;
import java.util.Set;

/**
 * A global index visitor knows how to extract 2 very important things
 * from an index table when given a query. It can do its best to find
 * cardinality information from each leaf in the query and it can
 * pull back a set of shards for each leaf.
 */
public interface GlobalIndexVisitor extends NodeVisitor {

    Map<AttributeIndexKey, Long> getCardinalities();

    Map<AttributeIndexKey, Set<String>> getShards();

    void exec();
}
