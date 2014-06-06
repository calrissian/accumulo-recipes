package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Collection;
import java.util.Iterator;

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
