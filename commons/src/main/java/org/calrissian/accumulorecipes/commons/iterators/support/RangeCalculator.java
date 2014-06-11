/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.iterators.support;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.*;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * This class is used to criteria the global indices to determine that set of ranges to use when querying the shard table. The RangeCalculator looks at each term
 * in the criteria to determine if it is a equivalence, range, or wildcard comparison, and queries the appropriate index to find the ranges for the terms which are
 * then cached. The final set of ranges is computed as the AST is traversed.
 */
public class RangeCalculator extends QueryParser {

    protected static Logger log = Logger.getLogger(RangeCalculator.class);
    private static String WILDCARD = ".*";
    private static String SINGLE_WILDCARD = "\\.";
    protected Connector c;
    protected Authorizations auths;
    protected Multimap<String, QueryTerm> termsCopy = HashMultimap.create();
    protected String indexTableName;
    protected String reverseIndexTableName;
    protected int queryThreads = 8;
    /* final results of index lookups, ranges for the shard table */
    protected Set<Range> result = null;
    /* map of field names to values found in the index */
    protected Multimap<String, String> indexEntries = HashMultimap.create();
    /* map of value in the index to the original criteria values */
    protected Map<String, String> indexValues = new HashMap<String, String>();
    /* map of values in the criteria to map keys used */
    protected Multimap<String, MapKey> originalQueryValues = HashMultimap.create();
    /* map of field name to cardinality */
    protected Map<String, Long> termCardinalities = new HashMap<String, Long>();
    /* cached results of all ranges found global index lookups */
    protected Map<MapKey, TermRange> globalIndexResults = new HashMap<MapKey, TermRange>();

    /**
     * @return set of ranges to use for the shard table
     */
    public Set<Range> getResult() {
        return result;
    }

    /**
     * @return map of field names to index field values
     */
    public Multimap<String, String> getIndexEntries() {
        return indexEntries;
    }

    public Map<String, String> getIndexValues() {
        return indexValues;
    }

    /**
     * @return Cardinality for each field name.
     */
    public Map<String, Long> getTermCardinalities() {
        return termCardinalities;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        boolean previouslyInOrContext = false;
        EvaluationContext ctx = null;
        if (null != data && data instanceof EvaluationContext) {
            ctx = (EvaluationContext) data;
            previouslyInOrContext = ctx.inOrContext;
        } else {
            ctx = new EvaluationContext();
        }
        ctx.inOrContext = true;
        // Process both sides of this node. Left branch first
        node.jjtGetChild(0).jjtAccept(this, ctx);
        Long leftCardinality = this.termCardinalities.get(ctx.lastProcessedTerm);
        if (null == leftCardinality)
            leftCardinality = 0L;
        TermRange leftRange = ctx.lastRange;
        if (log.isDebugEnabled())
            log.debug("[OR-left] term: " + ctx.lastProcessedTerm + ", cardinality: " + leftCardinality + ", ranges: " + leftRange.getRanges().size());

        // Process the right branch
        node.jjtGetChild(1).jjtAccept(this, ctx);
        Long rightCardinality = this.termCardinalities.get(ctx.lastProcessedTerm);
        if (null == rightCardinality)
            rightCardinality = 0L;
        TermRange rightRange = ctx.lastRange;
        if (log.isDebugEnabled())
            log.debug("[OR-right] term: " + ctx.lastProcessedTerm + ", cardinality: " + rightCardinality + ", ranges: " + rightRange.getRanges().size());

        // reset the state
        if (null != data && !previouslyInOrContext)
            ctx.inOrContext = false;
        // Add the ranges for the left and right branches to a TreeSet to sort them
        Set<Range> ranges = new TreeSet<Range>();
        ranges.addAll(leftRange.getRanges());
        ranges.addAll(rightRange.getRanges());
        // Now create the union set
        Set<Text> shardsAdded = new HashSet<Text>();
        Set<Range> returnSet = new HashSet<Range>();
        for (Range r : ranges) {
            if (!shardsAdded.contains(r.getStartKey().getRow())) {
                // Only add ranges with a start key for the entire shard.
                if (r.getStartKey().getColumnFamily() == null) {
                    shardsAdded.add(r.getStartKey().getRow());
                }
                returnSet.add(r);
            } else {
                // if (log.isTraceEnabled())
                log.info("Skipping event specific range: " + r.toString() + " because shard range has already been added: "
                        + shardsAdded.contains(r.getStartKey().getRow()));
            }
        }
        // Clear the ranges from the context and add the result in its place
        TermRange orRange = new TermRange("OR_RESULT", "foo");
        orRange.addAll(returnSet);
        if (log.isDebugEnabled())
            log.debug("[OR] results: " + orRange.getRanges().toString());
        ctx.lastRange = orRange;
        ctx.lastProcessedTerm = "OR_RESULT";
        this.termCardinalities.put("OR_RESULT", (leftCardinality + rightCardinality));
        return null;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        boolean previouslyInAndContext = false;
        EvaluationContext ctx = null;
        if (null != data && data instanceof EvaluationContext) {
            ctx = (EvaluationContext) data;
            previouslyInAndContext = ctx.inAndContext;
        } else {
            ctx = new EvaluationContext();
        }
        ctx.inAndContext = true;
        // Process both sides of this node.
        node.jjtGetChild(0).jjtAccept(this, ctx);
        String leftTerm = ctx.lastProcessedTerm;
        Long leftCardinality = this.termCardinalities.get(leftTerm);
        if (null == leftCardinality)
            leftCardinality = 0L;
        TermRange leftRange = ctx.lastRange;
        if (log.isDebugEnabled())
            log.debug("[AND-left] term: " + ctx.lastProcessedTerm + ", cardinality: " + leftCardinality + ", ranges: " + leftRange.getRanges().size());

        // Process the right branch
        node.jjtGetChild(1).jjtAccept(this, ctx);
        String rightTerm = ctx.lastProcessedTerm;
        Long rightCardinality = this.termCardinalities.get(rightTerm);
        if (null == rightCardinality)
            rightCardinality = 0L;
        TermRange rightRange = ctx.lastRange;
        if (log.isDebugEnabled())
            log.debug("[AND-right] term: " + ctx.lastProcessedTerm + ", cardinality: " + rightCardinality + ", ranges: " + rightRange.getRanges().size());

        // reset the state
        if (null != data && !previouslyInAndContext)
            ctx.inAndContext = false;

        long card = 0L;
        TermRange andRange = new TermRange("AND_RESULT", "foo");
        if ((leftCardinality > 0 && leftCardinality <= rightCardinality) || rightCardinality == 0) {
            card = leftCardinality;
            andRange.addAll(leftRange.getRanges());
        } else if ((rightCardinality > 0 && rightCardinality <= leftCardinality) || leftCardinality == 0) {
            card = rightCardinality;
            andRange.addAll(rightRange.getRanges());
        }
        if (log.isDebugEnabled())
            log.debug("[AND] results: " + andRange.getRanges().toString());
        ctx.lastRange = andRange;
        ctx.lastProcessedTerm = "AND_RESULT";
        this.termCardinalities.put("AND_RESULT", card);

        return null;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = false;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // Get the terms from the global index
        // Remove the begin and end ' marks
        String termValue = null;
        if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
            termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
        else
            termValue = (String) term.getValue();
        // Get the values found in the index for this criteria term
        TermRange ranges = null;
        for (MapKey key : this.originalQueryValues.get(termValue)) {
            if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
                ranges = this.globalIndexResults.get(key);
                if (log.isDebugEnabled())
                    log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
            }
        }
        // If no result for this field name and value, then add empty range
        if (null == ranges)
            ranges = new TermRange(fieldName.toString(), (String) term.getValue());
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = ranges;
            ctx.lastProcessedTerm = fieldName.toString();
        }
        return null;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = true;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        if (negated)
            negatedTerms.add(fieldName.toString());
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // We can only use the global index for equality, put in fake results
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = new TermRange(fieldName.toString(), term.getValue());
            ctx.lastProcessedTerm = fieldName.toString();
            termCardinalities.put(fieldName.toString(), 0L);
        }
        return null;
    }
//
//  /**
//   *
//   * @param indexRanges
//   * @param tableName
//   * @param isReverse
//   *          switch that determines whether or not to reverse the results
//   * @param override
//   *          mapKey for wildcard and range queries that specify which mapkey to use in the results
//   * @param typeFilter
//   *          - optional list of datatypes
//   * @throws TableNotFoundException
//   */
//  protected Map<MapKey,TermRange> queryGlobalIndex(Map<MapKey,Set<Range>> indexRanges, String specificFieldName, String tableName, boolean isReverse,
//                                                   MapKey override, Set<String> typeFilter) throws TableNotFoundException {
//
//    // The results map where the key is the field name and field value and the
//    // value is a set of ranges. The mapkey will always be the field name
//    // and field value that was passed in the original criteria. The TermRange
//    // will contain the field name and field value found in the index.
//    Map<MapKey,TermRange> results = new HashMap<MapKey,TermRange>();
//
//    // Seed the results map and create the range set for the batch scanner
//    Set<Range> rangeSuperSet = new HashSet<Range>();
//    for (Entry<MapKey,Set<Range>> entry : indexRanges.entrySet()) {
//      rangeSuperSet.addAll(entry.getValue());
//      TermRange tr = new TermRange(entry.getKey().getFieldName(), entry.getKey().getFieldValue());
//      if (null == override)
//        results.put(entry.getKey(), tr);
//      else
//        results.put(override, tr);
//    }
//
//    if (log.isDebugEnabled())
//      log.debug("Querying global index table: " + tableName + ", range: " + rangeSuperSet.toString() + " colf: " + specificFieldName);
//    BatchScanner bs = this.c.createBatchScanner(tableName, this.auths, this.queryThreads);
//    bs.setRanges(rangeSuperSet);
//    if (null != specificFieldName) {
//      bs.fetchColumnFamily(new Text(specificFieldName));
//    }
//
//    for (Entry<Key,Value> entry : bs) {
//      if (log.isDebugEnabled()) {
//        log.debug("Index entry: " + entry.getKey().toString());
//      }
//      String fieldValue = null;
//      if (!isReverse) {
//        fieldValue = entry.getKey().getRow().toString();
//      } else {
//        StringBuilder buf = new StringBuilder(entry.getKey().getRow().toString());
//        fieldValue = buf.reverse().toString();
//      }
//
//      String fieldName = entry.getKey().getColumnFamily().toString();
//      // Get the shard id and datatype from the colq
//      String colq = entry.getKey().getColumnQualifier().toString();
//      int separator = colq.indexOf(EvaluatingIterator.NULL_BYTE_STRING);
//      String shardId = null;
//      String datatype = null;
//      if (separator != -1) {
//        shardId = colq.substring(0, separator);
//        datatype = colq.substring(separator + 1);
//      } else {
//        shardId = colq;
//      }
//      // Skip this entry if the type is not correct
//      if (null != datatype && null != typeFilter && !typeFilter.contains(datatype))
//        continue;
//      // Parse the UID.List object from the value
//      Uid.List uidList = null;
//      try {
//        uidList = Uid.List.parseFrom(entry.getValue().get());
//      } catch (InvalidProtocolBufferException e) {
//        // Don't add UID information, at least we know what shards
//        // it is located in.
//      }
//
//      // Add the count for this shard to the total count for the term.
//      long count = 0;
//      Long storedCount = termCardinalities.get(fieldName);
//      if (null == storedCount || 0 == storedCount) {
//        count = uidList.getCOUNT();
//      } else {
//        count = uidList.getCOUNT() + storedCount;
//      }
//      termCardinalities.put(fieldName, count);
//      this.indexEntries.put(fieldName, fieldValue);
//
//      if (null == override)
//        this.indexValues.put(fieldValue, fieldValue);
//      else
//        this.indexValues.put(fieldValue, override.getOriginalQueryValue());
//
//      // Create the keys
//      Text shard = new Text(shardId);
//      if (uidList.getIGNORE()) {
//        // Then we create a scan range that is the entire shard
//        if (null == override)
//          results.get(new MapKey(fieldName, fieldValue)).add(new Range(shard));
//        else
//          results.get(override).add(new Range(shard));
//      } else {
//        // We should have UUIDs, create event ranges
//        for (String uuid : uidList.getUIDList()) {
//          Text cf = new Text(datatype);
//          TextUtil.textAppend(cf, uuid);
//          Key startKey = new Key(shard, cf);
//          Key endKey = new Key(shard, new Text(cf.toString() + EvaluatingIterator.NULL_BYTE_STRING));
//          Range eventRange = new Range(startKey, true, endKey, false);
//          if (null == override)
//            results.get(new MapKey(fieldName, fieldValue)).add(eventRange);
//          else
//            results.get(override).add(eventRange);
//        }
//      }
//    }
//    bs.close();
//    return results;
//  }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = false;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // Get the terms from the global index
        // Remove the begin and end ' marks
        String termValue = null;
        if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
            termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
        else
            termValue = (String) term.getValue();
        // Get the values found in the index for this criteria term
        TermRange ranges = null;
        for (MapKey key : this.originalQueryValues.get(termValue)) {
            if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
                ranges = this.globalIndexResults.get(key);
                if (log.isDebugEnabled())
                    log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
            }
        }
        // If no result for this field name and value, then add empty range
        if (null == ranges)
            ranges = new TermRange(fieldName.toString(), (String) term.getValue());
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = ranges;
            ctx.lastProcessedTerm = fieldName.toString();
        }
        return null;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = false;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // Get the terms from the global index
        // Remove the begin and end ' marks
        String termValue = null;
        if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
            termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
        else
            termValue = (String) term.getValue();
        // Get the values found in the index for this criteria term
        TermRange ranges = null;
        for (MapKey key : this.originalQueryValues.get(termValue)) {
            if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
                ranges = this.globalIndexResults.get(key);
                if (log.isDebugEnabled())
                    log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
            }
        }
        // If no result for this field name and value, then add empty range
        if (null == ranges)
            ranges = new TermRange(fieldName.toString(), (String) term.getValue());
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = ranges;
            ctx.lastProcessedTerm = fieldName.toString();
        }
        return null;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = false;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // Get the terms from the global index
        // Remove the begin and end ' marks
        String termValue = null;
        if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
            termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
        else
            termValue = (String) term.getValue();
        // Get the values found in the index for this criteria term
        TermRange ranges = null;
        for (MapKey key : this.originalQueryValues.get(termValue)) {
            if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
                ranges = this.globalIndexResults.get(key);
                if (log.isDebugEnabled())
                    log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
            }
        }
        // If no result for this field name and value, then add empty range
        if (null == ranges)
            ranges = new TermRange(fieldName.toString(), (String) term.getValue());
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = ranges;
            ctx.lastProcessedTerm = fieldName.toString();
        }
        return null;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = false;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // Get the terms from the global index
        // Remove the begin and end ' marks
        String termValue = null;
        if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
            termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
        else
            termValue = (String) term.getValue();
        // Get the values found in the index for this criteria term
        TermRange ranges = null;
        for (MapKey key : this.originalQueryValues.get(termValue)) {
            if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
                ranges = this.globalIndexResults.get(key);
                if (log.isDebugEnabled())
                    log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
            }
        }
        // If no result for this field name and value, then add empty range
        if (null == ranges)
            ranges = new TermRange(fieldName.toString(), (String) term.getValue());
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = ranges;
            ctx.lastProcessedTerm = fieldName.toString();
        }
        return null;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = false;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // Get the terms from the global index
        // Remove the begin and end ' marks
        String termValue = null;
        if (((String) term.getValue()).startsWith("'") && ((String) term.getValue()).endsWith("'"))
            termValue = ((String) term.getValue()).substring(1, ((String) term.getValue()).length() - 1);
        else
            termValue = (String) term.getValue();
        // Get the values found in the index for this criteria term
        TermRange ranges = null;
        for (MapKey key : this.originalQueryValues.get(termValue)) {
            if (key.getFieldName().equalsIgnoreCase(fieldName.toString())) {
                ranges = this.globalIndexResults.get(key);
                if (log.isDebugEnabled())
                    log.debug("Results for cached index ranges for key: " + key + " are " + ranges);
            }
        }
        // If no result for this field name and value, then add empty range
        if (null == ranges)
            ranges = new TermRange(fieldName.toString(), (String) term.getValue());
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = ranges;
            ctx.lastProcessedTerm = fieldName.toString();
        }

        return null;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        StringBuilder fieldName = new StringBuilder();
        ObjectHolder value = new ObjectHolder();
        // Process both sides of this node.
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        // Ignore functions in the criteria
        if (left instanceof FunctionResult || right instanceof FunctionResult)
            return null;
        decodeResults(left, right, fieldName, value);
        // We need to check to see if we are in a NOT context. If so,
        // then we need to reverse the negation.
        boolean negated = true;
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            if (ctx.inNotContext)
                negated = !negated;
        }
        if (negated)
            negatedTerms.add(fieldName.toString());
        QueryTerm term = new QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
        termsCopy.put(fieldName.toString(), term);
        // We can only use the global index for equality, put in fake results
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = new TermRange(fieldName.toString(), term.getValue());
            ctx.lastProcessedTerm = fieldName.toString();
            termCardinalities.put(fieldName.toString(), 0L);
        }

        return null;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = new TermRange("null", "null");
            ctx.lastProcessedTerm = "null";
            termCardinalities.put("null", 0L);
        }
        return new LiteralResult(node.image);
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = new TermRange("true", "true");
            ctx.lastProcessedTerm = "true";
            termCardinalities.put("true", 0L);
        }
        return new LiteralResult(node.image);
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = new TermRange("false", "false");
            ctx.lastProcessedTerm = "false";
            termCardinalities.put("false", 0L);
        }
        return new LiteralResult(node.image);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // objectNode 0 is the prefix
        // objectNode 1 is the identifier , the others are parameters.
        // process the remaining arguments
        FunctionResult fr = new FunctionResult();
        int argc = node.jjtGetNumChildren() - 2;
        for (int i = 0; i < argc; i++) {
            // Process both sides of this node.
            Object result = node.jjtGetChild(i + 2).jjtAccept(this, data);
            if (result instanceof TermResult) {
                TermResult tr = (TermResult) result;
                fr.getTerms().add(tr);
                termsCopy.put((String) tr.value, null);
            }
        }
        if (null != data && data instanceof EvaluationContext) {
            EvaluationContext ctx = (EvaluationContext) data;
            ctx.lastRange = new TermRange(node.jjtGetChild(0).image, node.jjtGetChild(1).image);
            ctx.lastProcessedTerm = node.jjtGetChild(0).image;
            termCardinalities.put(node.jjtGetChild(0).image, 0L);
        }
        return fr;
    }

    /**
     * Container used as map keys in this class
     */
    public static class MapKey implements Comparable<MapKey> {
        private String fieldName = null;
        private String fieldValue = null;
        private String originalQueryValue = null;

        public MapKey(String fieldName, String fieldValue) {
            super();
            this.fieldName = fieldName;
            this.fieldValue = fieldValue;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldValue() {
            return fieldValue;
        }

        public void setFieldValue(String fieldValue) {
            this.fieldValue = fieldValue;
        }

        public String getOriginalQueryValue() {
            return originalQueryValue;
        }

        public void setOriginalQueryValue(String originalQueryValue) {
            this.originalQueryValue = originalQueryValue;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(fieldName).append(fieldValue).toHashCode();
        }

        @Override
        public String toString() {
            return this.fieldName + " " + this.fieldValue;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null)
                return false;
            if (other instanceof MapKey) {
                MapKey o = (MapKey) other;
                return (this.fieldName.equals(o.fieldName) && this.fieldValue.equals(o.fieldValue));
            } else
                return false;
        }

        public int compareTo(MapKey o) {
            int result = this.fieldName.compareTo(o.fieldName);
            if (result != 0) {
                return this.fieldValue.compareTo(o.fieldValue);
            } else {
                return result;
            }
        }

    }

    /**
     * Container used to hold the lower and upper bound of a range
     */
    public static class RangeBounds {
        private String originalLower = null;
        private Text lower = null;
        private String originalUpper = null;
        private Text upper = null;

        public Text getLower() {
            return lower;
        }

        public void setLower(Text lower) {
            this.lower = lower;
        }

        public Text getUpper() {
            return upper;
        }

        public void setUpper(Text upper) {
            this.upper = upper;
        }

        public String getOriginalLower() {
            return originalLower;
        }

        public void setOriginalLower(String originalLower) {
            this.originalLower = originalLower;
        }

        public String getOriginalUpper() {
            return originalUpper;
        }

        public void setOriginalUpper(String originalUpper) {
            this.originalUpper = originalUpper;
        }
    }

    /**
     * Object that is used to hold ranges found in the index. Subclasses may compute the final range set in various ways.
     */
    protected static class TermRange implements Comparable<TermRange> {

        private String fieldName = null;
        private Object fieldValue = null;
        private Set<Range> ranges = new TreeSet<Range>();

        public TermRange(String name, Object fieldValue) {
            this.fieldName = name;
            this.fieldValue = fieldValue;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public Object getFieldValue() {
            return this.fieldValue;
        }

        public void addAll(Set<Range> r) {
            ranges.addAll(r);
        }

        public void add(Range r) {
            ranges.add(r);
        }

        public Set<Range> getRanges() {
            return ranges;
        }

        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("fieldName", fieldName);
            tsb.append("fieldValue", fieldValue);
            tsb.append("ranges", ranges);
            return tsb.toString();
        }

        public int compareTo(TermRange o) {
            int result = this.fieldName.compareTo(o.fieldName);
            if (result == 0) {
                return ((Integer) ranges.size()).compareTo(o.ranges.size());
            } else {
                return result;
            }
        }
    }

    /**
     * Object used to store context information as the AST is being traversed.
     */
    static class EvaluationContext {
        boolean inOrContext = false;
        boolean inNotContext = false;
        boolean inAndContext = false;
        TermRange lastRange = null;
        String lastProcessedTerm = null;
    }

}
