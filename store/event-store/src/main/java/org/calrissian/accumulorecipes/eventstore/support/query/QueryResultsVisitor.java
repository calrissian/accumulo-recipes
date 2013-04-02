package org.calrissian.accumulorecipes.eventstore.support.query;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.accumulorecipes.eventstore.support.QueryNodeHelper;
import org.calrissian.accumulorecipes.eventstore.support.query.validators.NoAndOrValidator;
import org.calrissian.accumulorecipes.eventstore.support.query.validators.NoOrNotEqualsValidator;
import org.calrissian.criteria.domain.*;
import org.calrissian.criteria.visitor.CollapseParentClauseVisitor;
import org.calrissian.criteria.visitor.EmptyParentCollapseVisitor;
import org.calrissian.criteria.visitor.NodeVisitor;
import org.calrissian.criteria.visitor.SingleClauseCollapseVisitor;
import org.calrissian.mango.collect.CloseableIterator;
import org.calrissian.mango.collect.ConcatCloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

import static org.calrissian.criteria.utils.NodeUtils.isLeaf;
import static org.calrissian.criteria.utils.NodeUtils.parentContainsOnlyLeaves;

/**
 * Visit query to validate and transform to perform the query against the swift event service.
 */
public class QueryResultsVisitor implements NodeVisitor {
    private static final Logger logger = LoggerFactory.getLogger(QueryResultsVisitor.class);

    protected Date start, end;
    protected Authorizations auths;
    protected CloseableIterator<Event> iterator;
    private QueryNodeHelper queryHelper;

    public QueryResultsVisitor(Node query, QueryNodeHelper queryHelper, Date start, Date end, Authorizations auths) {
        this.queryHelper = queryHelper;
        this.start = start;
        this.end = end;
        this.auths = auths;

        init(query);
    }

    protected void init(Node query) {
        query.accept(new SingleClauseCollapseVisitor());
        query.accept(new EmptyParentCollapseVisitor());
        query.accept(new CollapseParentClauseVisitor());

        //validators
        query.accept(new NoAndOrValidator());
        query.accept(new NoOrNotEqualsValidator());

        //develop query
        query.accept(this);
    }

    @Override
    public void begin(ParentNode node) {
    }

    @Override
    public void end(ParentNode node) {
        try {
            if (parentContainsOnlyLeaves(node)) {
                Node n = node;
                //trim and(eq) to eq
                if (node instanceof AndNode && node.children() != null && node.children().size() == 1) {
                    Node child = node.children().get(0);
                    if(isLeaf(child)) {
                        n = child;
                    }
                }
                populateIterator(n);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void populateIterator(Node node) throws IOException {
        if (node instanceof AndNode) {
            CloseableIterator<Event> query = andResultsIterator((AndNode) node);
            if (iterator != null) {
                //assume OR
                iterator = new ConcatCloseableIterator<Event>(Lists.newArrayList(query, iterator));
            } else {
                iterator = query;
            }
        } else if (node instanceof OrNode) {
            for (Node child : node.children()) {
                populateIterator(child);
            }
        } else if (node instanceof Leaf) {
            CloseableIterator<Event> query = leafResultsIterator((Leaf) node);
            if (iterator != null) {
                //assume OR
                iterator = new ConcatCloseableIterator<Event>(Lists.newArrayList(query, iterator));
            } else {
                iterator = query;
            }
        }
    }

    @Override
    public void visit(Leaf node) {
    }

    public CloseableIterator<Event> getResults() {
        return iterator;
    }

    protected CloseableIterator<Event> andResultsIterator(AndNode andNode) throws IOException {
        try {
            return queryHelper.queryAndNode(start, end, andNode, auths);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected CloseableIterator<Event> leafResultsIterator(Leaf node) throws IOException {
        try {
            return queryHelper.querySingleLeaf(start, end, node, auths);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
