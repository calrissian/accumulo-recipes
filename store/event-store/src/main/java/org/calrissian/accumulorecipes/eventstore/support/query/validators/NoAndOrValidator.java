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
