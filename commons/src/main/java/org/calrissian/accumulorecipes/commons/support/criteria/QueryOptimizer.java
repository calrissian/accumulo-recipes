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
package org.calrissian.accumulorecipes.commons.support.criteria;

import org.calrissian.accumulorecipes.commons.support.criteria.validators.MultipleEqualsValidator;
import org.calrissian.accumulorecipes.commons.support.criteria.validators.NoAndOrValidator;
import org.calrissian.accumulorecipes.commons.support.criteria.validators.NoOrNotEqualsValidator;
import org.calrissian.accumulorecipes.commons.support.criteria.validators.QueryKeysExtractorVisitor;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.CollapseParentClauseVisitor;
import org.calrissian.mango.criteria.visitor.EmptyParentCollapseVisitor;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.criteria.visitor.SingleClauseCollapseVisitor;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.calrissian.mango.collect.CloseableIterables.chain;

/**
 * Visit criteria to validate and transform to perform the criteria against the swift event service.
 */
public class QueryOptimizer implements NodeVisitor {

    protected Node node;
    protected Set<String> keysInQuery;

    public QueryOptimizer(Node query) {
      checkNotNull(query);

      this.node = query;  // TODO: Really, this should be cloned all the way

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

      QueryKeysExtractorVisitor extractKeysVisitor = new QueryKeysExtractorVisitor();
      query.accept(extractKeysVisitor);
      keysInQuery = extractKeysVisitor.getKeysFound();

      //develop criteria
      query.accept(this);
    }

    public Node getOptimizedQuery() {
      return this.node;
    }

    public Set<String> getKeysInQuery() {
      return keysInQuery;
    }

    @Override
    public void begin(ParentNode node) {
    }

    @Override
    public void end(ParentNode parentNode) {

    }

    @Override
    public void visit(Leaf node) {
    }
}
