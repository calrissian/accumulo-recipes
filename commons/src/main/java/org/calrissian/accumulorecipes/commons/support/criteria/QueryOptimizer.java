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
package org.calrissian.accumulorecipes.commons.support.criteria;

import org.calrissian.accumulorecipes.commons.support.criteria.visitors.*;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.visitor.CollapseParentClauseVisitor;
import org.calrissian.mango.criteria.visitor.EmptyParentCollapseVisitor;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.criteria.visitor.SingleClauseCollapseVisitor;
import org.calrissian.mango.types.TypeRegistry;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.mango.criteria.support.NodeUtils.isEmpty;

/**
 * Visit criteria to validate and transform to perform the criteria against the swift event service.
 */
public class QueryOptimizer implements NodeVisitor {

    protected Node node;
    protected Set<String> keysInQuery;
    protected GlobalIndexVisitor indexVisitor;
    protected TypeRegistry<String> typeRegistry;

    public QueryOptimizer(Node query, GlobalIndexVisitor indexVisitor, TypeRegistry<String> typeRegistry) {
        checkNotNull(query);

        this.node = query.clone(null);  // cloned so that original is not modified during optimization
        this.indexVisitor = indexVisitor;
        this.typeRegistry = typeRegistry;

        init();
    }

    public QueryOptimizer(Node query, TypeRegistry<String> typeRegistry) {
        this(query, null, typeRegistry);
    }

    protected void init() {

        if (!isEmpty(node)) {

            /**
             * This performs a cardinality optimization
             */
            if (indexVisitor != null) {
                node.accept(indexVisitor);
                indexVisitor.exec();
                node.accept(new CardinalityReorderVisitor(indexVisitor.getCardinalities(), typeRegistry));
            }

            Node previous = node.clone(null);
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

        //visitors
        query.accept(new NoOrNotEqualsValidator());

        QueryKeysExtractorVisitor extractKeysVisitor = new QueryKeysExtractorVisitor();
        query.accept(extractKeysVisitor);
        keysInQuery = extractKeysVisitor.getKeysFound();

        //develop criteria
        query.accept(this);
    }

    public Node getOptimizedQuery() {
        return this.node;
    }

    public Set<String> getKeysInQuery() {
        return keysInQuery;
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
            return indexVisitor.getShards();
        else
            throw new RuntimeException("A global index visitor was not configured on this optimizer.");
    }
}
