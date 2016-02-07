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

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Collection;

/**
 * Any In and NotIn nodes can be expressed as Or and And nodes respectively. This visitor makes the querying planning
 * easier by extracting the In and NotIn nodes to their respective And/Or NotEq/Eq combinations.
 */
public class ExtractInNotInVisitor implements NodeVisitor {
    @Override
    public void begin(ParentNode parentNode) {
    }

    @Override
    public void end(ParentNode parentNode) {
    }

    @Override
    public void visit(Leaf leaf) {

        if(leaf instanceof InLeaf) {

            InLeaf node = (InLeaf)leaf;
            node.parent().removeChild(node);

            OrNode orNode = new OrNode(leaf.parent());
            for (Object obj : (Collection)node.getValue()) {
                orNode.addChild(new EqualsLeaf<Object>(node.getTerm(), obj, orNode));
            }
            leaf.parent().addChild(orNode);
        }

        else if(leaf instanceof NotInLeaf) {

            NotInLeaf node = (NotInLeaf)leaf;
            node.parent().removeChild(node);

            AndNode andNode = new AndNode(leaf.parent());
            for (Object obj : (Collection)node.getValue()) {
                andNode.addChild(new NotEqualsLeaf<Object>(node.getTerm(), obj, andNode));
            }
            leaf.parent().addChild(andNode);
        }
    }
}
