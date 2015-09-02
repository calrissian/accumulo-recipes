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
package org.calrissian.accumulorecipes.commons.support.qfd.planner;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static org.calrissian.mango.criteria.support.NodeUtils.isEmpty;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.CalculateShardsVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.CardinalityReorderVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.ExtractInNotInVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.ExtractRangesVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.NoOrNotEqualsValidator;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.RangeSplitterVisitor;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.visitor.CollapseParentClauseVisitor;
import org.calrissian.mango.criteria.visitor.EmptyParentCollapseVisitor;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.criteria.visitor.SingleClauseCollapseVisitor;
import org.calrissian.mango.types.TypeRegistry;

/**
 * Visit criteria to validate and config to perform the criteria against the event service.
 */
public class QueryPlanner implements NodeVisitor {

    protected Node node;
    protected Set<String> shards = newHashSet();
    protected GlobalIndexVisitor indexVisitor;
    protected TypeRegistry<String> typeRegistry;

    public QueryPlanner(Node query, GlobalIndexVisitor indexVisitor, TypeRegistry<String> typeRegistry) {
        checkNotNull(query);

        this.node = query.clone(null);  // cloned so that original is not modified during optimization
        this.indexVisitor = indexVisitor;
        this.typeRegistry = typeRegistry;

        init();
    }

    public QueryPlanner(Node query, TypeRegistry<String> typeRegistry) {
        this(query, null, typeRegistry);
    }

    protected void init() {

        if (!isEmpty(node)) {

            node.accept(new ExtractInNotInVisitor());

            if (indexVisitor != null) {

                /**
                 * Optimizes the query using cardinalities by reordering it to minimize the number of seeks
                 */
                node.accept(indexVisitor);
                indexVisitor.exec();
                node.accept(new CardinalityReorderVisitor(indexVisitor.getCardinalities(), typeRegistry));

                /**
                 * Tries to minimize the number of shards that need to be scanned
                 */
                CalculateShardsVisitor calculateShardsVisitor = new CalculateShardsVisitor(indexVisitor.getShards(), typeRegistry);
                node.accept(calculateShardsVisitor);
                shards = calculateShardsVisitor.getShards();
            }

            /**
             * Run optimizations until the query doesn't change
             * (this makes sure the optimizations are applied fully for the query)
             */
            Node previous = node.clone(null);
            runOptimizations(node);
            while(!previous.equals(node)) {
                runOptimizations(node);
                previous = node.clone(null);
            }

        }
    }

    protected void runOptimizations(Node query) {

        query.accept(new SingleClauseCollapseVisitor());
        query.accept(new EmptyParentCollapseVisitor());
        query.accept(new CollapseParentClauseVisitor());
        query.accept(new RangeSplitterVisitor());

        ExtractRangesVisitor extractRangesVisitor = new ExtractRangesVisitor();
        query.accept(extractRangesVisitor);
        extractRangesVisitor.extract(); // perform actual extraction

        /**
         * Validators to throw exceptions if query is not structured according to given rules
         */
        query.accept(new NoOrNotEqualsValidator());

        //develop criteria
        query.accept(this);
    }

    public Node getOptimizedQuery() {
        return this.node;
    }

    @Override
    public void begin(ParentNode node) {
    }

    @Override
    public void end(ParentNode parentNode) {

    }

    @Override
    public void visit(Leaf node) {
    }

    public Set<String> getShards() {

        if (indexVisitor != null)
            return shards;
        else
            throw new RuntimeException("A global index visitor was not configured on this optimizer.");
    }
}
