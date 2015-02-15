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

import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.criteria.domain.OrNode;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

/**
 * Current, it's not valid to have a NotEqualsLead in an Or node
 */
public class NoOrNotEqualsValidator implements NodeVisitor {

    @Override
    public void begin(ParentNode node) {
    }

    @Override
    public void end(ParentNode node) {

    }

    @Override
    public void visit(Leaf node) {
        if (node instanceof NotEqualsLeaf && node.parent() instanceof OrNode) {
            throw new IllegalArgumentException("Cannot have a not equals under an or node");
        }
    }
}
