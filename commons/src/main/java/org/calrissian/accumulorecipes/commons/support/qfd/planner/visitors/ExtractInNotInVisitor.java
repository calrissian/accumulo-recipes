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

import java.util.Collection;
import java.util.Iterator;

import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.EqualsLeaf;
import org.calrissian.mango.criteria.domain.InLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.criteria.domain.NotInLeaf;
import org.calrissian.mango.criteria.domain.OrNode;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

/**
 * Any In and NotIn nodes can be expressed as Or and And nodes respectively. This visitor makes the querying planning
 * easier by extracting the In and NotIn nodes to their respective And/Or NotEq/Eq combinations.
 */
public class ExtractInNotInVisitor implements NodeVisitor
{
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

            OrNode andNode = new OrNode(leaf.parent());
            Iterator<Object> objs = ((Collection<Object>)node.getValue()).iterator();
            while(objs.hasNext()) {
                Object curObj = objs.next();
                andNode.addChild(new EqualsLeaf(node.getKey(), curObj, andNode));
            }

            leaf.parent().addChild(andNode);
        }

        else if(leaf instanceof NotInLeaf) {

            NotInLeaf node = (NotInLeaf)leaf;
            node.parent().removeChild(node);

            AndNode andNode = new AndNode(leaf.parent());
            Iterator<Object> objs = ((Collection<Object>)node.getValue()).iterator();
            while(objs.hasNext()) {
                Object curObj = objs.next();
                andNode.addChild(new NotEqualsLeaf(node.getKey(), curObj, andNode));
            }

            leaf.parent().addChild(andNode);
        }
    }
}
