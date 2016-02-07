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

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Arrays;

/**
 * This visitor pulls all bounded ranges into unbounded ranges.
 */
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
            RangeLeaf rangeLeaf = (RangeLeaf) leaf;
            GreaterThanEqualsLeaf lhs =
                    new GreaterThanEqualsLeaf<Object>(rangeLeaf.getTerm(), rangeLeaf.getStart(), rangeLeaf.parent());
            LessThanEqualsLeaf rhs =
                    new LessThanEqualsLeaf<Object>(rangeLeaf.getTerm(), rangeLeaf.getEnd(), rangeLeaf.parent());

            leaf.parent().removeChild(leaf);
            if (leaf.parent() instanceof AndNode) {
                leaf.parent().addChild(lhs);
                leaf.parent().addChild(rhs);
            } else {
                AndNode node = new AndNode(leaf.parent(), Arrays.<Node>asList(lhs, rhs));
                leaf.parent().addChild(node);
            }
        }
    }
}
