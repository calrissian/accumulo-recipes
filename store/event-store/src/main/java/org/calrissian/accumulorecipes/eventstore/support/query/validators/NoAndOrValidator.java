package org.calrissian.accumulorecipes.eventstore.support.query.validators;

import org.calrissian.criteria.domain.AndNode;
import org.calrissian.criteria.domain.Leaf;
import org.calrissian.criteria.domain.OrNode;
import org.calrissian.criteria.domain.ParentNode;
import org.calrissian.criteria.visitor.NodeVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 11/13/12
 * Time: 3:24 PM
 */
public class NoAndOrValidator implements NodeVisitor {
    private static final Logger logger = LoggerFactory.getLogger(NoAndOrValidator.class);

    @Override
    public void begin(ParentNode node) {
        if (node instanceof OrNode && node.parent() instanceof AndNode)
            throw new IllegalArgumentException("And Node cannot contain an Or Node");
    }

    @Override
    public void end(ParentNode node) {

    }

    @Override
    public void visit(Leaf node) {

    }
}
