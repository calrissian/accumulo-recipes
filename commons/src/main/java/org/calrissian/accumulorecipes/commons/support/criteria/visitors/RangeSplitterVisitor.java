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

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import static java.util.Arrays.asList;

public class RangeSplitterVisitor implements NodeVisitor {

    @Override
    public void begin(ParentNode parentNode) {
    }

    @Override
    public void end(ParentNode parentNode) {
    }

    @Override
    public void visit(Leaf leaf) {

        if (leaf instanceof RangeLeaf) {
            GreaterThanEqualsLeaf lhs =
                    new GreaterThanEqualsLeaf(((RangeLeaf) leaf).getKey(), ((RangeLeaf) leaf).getStart(), leaf.parent());
            LessThanEqualsLeaf rhs =
                    new LessThanEqualsLeaf(((RangeLeaf) leaf).getKey(), ((RangeLeaf) leaf).getEnd(), leaf.parent());

            leaf.parent().removeChild(leaf);
            if (leaf.parent() instanceof AndNode) {
                leaf.parent().addChild(lhs);
                leaf.parent().addChild(rhs);
            } else {
                AndNode node = new AndNode(leaf.parent(), asList(new Node[]{lhs, rhs}));
                leaf.parent().addChild(node);
            }
        }
    }
}
