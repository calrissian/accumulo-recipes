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
package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.EqualsLeaf;
import org.calrissian.mango.criteria.domain.GreaterThanLeaf;
import org.calrissian.mango.criteria.domain.Node;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExtractRangesVisitorTest {

  @Test
  public void testExtract_onlyRanges() {

    Node node = new QueryBuilder().and().greaterThan("key", "val").lessThan("key", "val").end().build();


    ExtractRangesVisitor rangesVisitor = new ExtractRangesVisitor();
    node.accept(rangesVisitor);

    rangesVisitor.extract();

    assertEquals(1, node.children().size());
    assertTrue(node.children().get(0) instanceof GreaterThanLeaf);
  }

  @Test
  public void testExtract_rangesAndNonRanges() {

    Node node = new QueryBuilder().and().greaterThan("key", "val").lessThan("key", "val").eq("key", "val").end().build();

    ExtractRangesVisitor rangesVisitor = new ExtractRangesVisitor();
    node.accept(rangesVisitor);

    rangesVisitor.extract();

    assertEquals(1, node.children().size());
    assertTrue(node.children().get(0) instanceof EqualsLeaf);
  }

}
