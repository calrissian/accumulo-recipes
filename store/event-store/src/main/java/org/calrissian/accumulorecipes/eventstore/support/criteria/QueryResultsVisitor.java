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
package org.calrissian.accumulorecipes.eventstore.support.criteria;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.eventstore.support.QueryNodeHelper;
import org.calrissian.accumulorecipes.eventstore.support.criteria.validators.MultipleEqualsValidator;
import org.calrissian.accumulorecipes.eventstore.support.criteria.validators.NoAndOrValidator;
import org.calrissian.accumulorecipes.eventstore.support.criteria.validators.NoOrNotEqualsValidator;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.CollapseParentClauseVisitor;
import org.calrissian.mango.criteria.visitor.EmptyParentCollapseVisitor;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.criteria.visitor.SingleClauseCollapseVisitor;

import java.io.IOException;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.calrissian.mango.collect.CloseableIterables.chain;
import static org.calrissian.mango.criteria.utils.NodeUtils.isLeaf;
import static org.calrissian.mango.criteria.utils.NodeUtils.parentContainsOnlyLeaves;

/**
 * Visit criteria to validate and transform to perform the criteria against the swift event service.
 */
public class QueryResultsVisitor implements NodeVisitor {

    protected Date start, end;
    protected Authorizations auths;
    protected CloseableIterable<StoreEntry> iterable = null;
    private QueryNodeHelper queryHelper;

    public QueryResultsVisitor(Node query, QueryNodeHelper queryHelper, Date start, Date end, Authorizations auths) {
        checkNotNull(query);
        checkNotNull(queryHelper);
        checkNotNull(start);
        checkNotNull(end);
        checkNotNull(auths);

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
        query.accept(new MultipleEqualsValidator());

        //develop criteria
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
                populateIterable(n);
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void populateIterable(Node node) throws IOException {
        if (node instanceof AndNode) {
            CloseableIterable<StoreEntry> query = andResults((AndNode) node);
            if (iterable != null) {
                //assume OR
                iterable = chain(newArrayList(query, iterable));

            } else {
                iterable = query;
            }
        } else if (node instanceof OrNode) {
            for (Node child : node.children()) {
                populateIterable(child);
            }
        } else if (node instanceof Leaf) {
            CloseableIterable<StoreEntry> query = leafResults((Leaf) node);
            if (iterable != null) {
                //assume OR
                iterable = chain(newArrayList(query, iterable));
            } else {
                iterable = query;
            }
        }
    }

    @Override
    public void visit(Leaf node) {
    }

    public CloseableIterable<StoreEntry> getResults() {
        return iterable;
    }

    protected CloseableIterable<StoreEntry> andResults(AndNode andNode) throws IOException {
        try {
            return queryHelper.queryAndNode(start, end, andNode, auths);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected CloseableIterable<StoreEntry> leafResults(Leaf node) throws IOException {
        try {
            return queryHelper.querySingleLeaf(start, end, node, auths);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
