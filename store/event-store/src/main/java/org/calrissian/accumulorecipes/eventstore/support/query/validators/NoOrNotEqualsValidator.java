package org.calrissian.accumulorecipes.eventstore.support.query.validators;

import org.calrissian.criteria.domain.Leaf;
import org.calrissian.criteria.domain.NotEqualsLeaf;
import org.calrissian.criteria.domain.OrNode;
import org.calrissian.criteria.domain.ParentNode;
import org.calrissian.criteria.visitor.NodeVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NoOrNotEqualsValidator implements NodeVisitor {
    private static final Logger logger = LoggerFactory.getLogger(NoOrNotEqualsValidator.class);

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
