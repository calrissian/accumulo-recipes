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
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.domain.TermLeaf;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.HashSet;
import java.util.Set;

/**
 * This class will extract all the keys used anywhere in a query. This can be helpful
 * for metrics and statistical analysis of queries.
 */
public class QueryKeysExtractorVisitor implements NodeVisitor {

    private Set<String> keysFound = new HashSet<String>();

    @Override
    public void begin(ParentNode parentNode) {
    }

    @Override
    public void end(ParentNode parentNode) {
    }

    @Override
    public void visit(Leaf leaf) {
        keysFound.add(((TermLeaf) leaf).getTerm());
    }

    public Set<String> getKeysFound() {
        return keysFound;
    }
}
