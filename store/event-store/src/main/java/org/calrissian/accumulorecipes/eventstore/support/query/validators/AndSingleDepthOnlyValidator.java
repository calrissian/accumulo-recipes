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
package org.calrissian.accumulorecipes.eventstore.support.query.validators;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.calrissian.criteria.domain.*;
import org.calrissian.criteria.utils.NodeUtils;
import org.calrissian.criteria.visitor.NodeVisitor;

import java.util.Collection;

/**
 * Will validate that the query is only And with 2 or more leaves.
 * <p/>
 * Date: 12/18/12
 * Time: 8:21 AM
 */
public class AndSingleDepthOnlyValidator implements NodeVisitor {
    @Override
    public void begin(ParentNode node) {
        if (!(node instanceof AndNode)) throw new IllegalArgumentException("Not an And Node");

        int size = node.getNodes().size();
        if (node.getNodes() == null || size < 2)
            throw new IllegalArgumentException("At least 2 query leaves expected");

        if (!NodeUtils.parentContainsOnlyLeaves(node))
            throw new IllegalArgumentException("Only Leaf nodes expected. Not a single depth node");

        //make sure not all are NotEqual nodes
        Collection<Node> notEqNodes = Collections2.filter(node.getNodes(), new Predicate<Node>() {
            @Override
            public boolean apply(Node node) {
                return node instanceof NotEqualsLeaf;
            }
        });

        if(notEqNodes != null && notEqNodes.size() == size) {
            throw new IllegalArgumentException("Not every leaf can be a not equals");
        }
    }

    @Override
    public void end(ParentNode node) {

    }

    @Override
    public void visit(Leaf node) {

    }
}
