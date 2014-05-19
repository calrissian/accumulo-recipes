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

import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ExtractRangesVisitor implements NodeVisitor {

    private boolean nonRangeFound = false;
    private List<Leaf> rangeNodes = new LinkedList<Leaf>();

    @Override
    public void begin(ParentNode parentNode) {
    }

    @Override
    public void end(ParentNode parentNode) {

    }

    public void extract() {

        if (rangeNodes.size() > 0) {
            Iterator<Leaf> nodeIter = rangeNodes.iterator();
            if (!nonRangeFound)
                nodeIter.next();
            while (nodeIter.hasNext()) {
                Leaf leaf = nodeIter.next();
                leaf.parent().removeChild(leaf);
            }
        }
    }

    @Override
    public void visit(Leaf leaf) {
        if (NodeUtils.isRangeLeaf(leaf))
            rangeNodes.add(leaf);
        else
            nonRangeFound = true;
    }
}
