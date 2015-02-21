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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Multimap;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields.FieldValue;


/**
 * This class evaluates events against a criteria. The criteria is passed to the constructor and then parsed. It is evaluated against an event in the evaluate method.
 */
public class QueryEvaluator {

    private static Logger log = Logger.getLogger(QueryEvaluator.class);
    // According to the JEXL 2.0 docs, the engine is thread-safe. Let's create 1 engine per VM and
    // cache 128 expressions
    private static JexlEngine engine = new JexlEngine(null, new Arithmetic(false), null, null);

    static {
        engine.setSilent(false);
        engine.setCache(128);
    }

    private String query = null;
    private Set<String> literals = null;
    private Multimap<String, QueryParser.QueryTerm> terms = null;
    private String modifiedQuery = null;
    private JexlContext ctx = new MapContext();

    public QueryEvaluator(String query) throws ParseException {
        this.query = query;
        QueryParser parser = new QueryParser();
        parser.execute(query);
        this.terms = parser.getQueryTerms();
        this.literals = parser.getQueryIdentifiers();
    }

    public String getQuery() {
        return this.query;
    }

    public StringBuilder rewriteQuery(StringBuilder query, String fieldName, Collection<FieldValue> fieldValues) {
        if (log.isDebugEnabled()) {
            log.debug("rewriteQuery");
        }

        if (log.isDebugEnabled()) {
            log.debug("Modifying original criteria: " + query);
        }
        // Pull the values out of the FieldValue object
        String[] values = new String[fieldValues.size()];
        int idx = 0;
        for (FieldValue fv : fieldValues) {
            values[idx] = new String(fv.getValue());
            idx++;
        }
        // Add the array to the context
        ctx.set(fieldName, values);

        Collection<QueryParser.QueryTerm> qt = terms.get(fieldName);

        // Add a script to the beginning of the criteria for this multi-valued field
        StringBuilder script = new StringBuilder();
        script.append("_").append(fieldName).append(" = false;\n");
        script.append("for (field : ").append(fieldName).append(") {\n");

        for (QueryParser.QueryTerm t : qt) {
            if (!t.getOperator().equals(JexlOperatorConstants.getOperator(ParserTreeConstants.JJTFUNCTIONNODE))) {
                script.append("\tif (_").append(fieldName).append(" == false && field ").append(t.getOperator()).append(" ").append(t.getValue()).append(") { \n");
            } else {
                script.append("\tif (_").append(fieldName).append(" == false && ").append(t.getValue().toString().replace(fieldName, "field")).append(") { \n");
            }
            script.append("\t\t_").append(fieldName).append(" = true;\n");
            script.append("\t}\n");
        }
        script.append("}\n");

        // Add the script to the beginning of the criteria
        query.insert(0, script.toString());

        StringBuilder newPredicate = new StringBuilder();
        newPredicate.append("_").append(fieldName).append(" == true");

        for (QueryParser.QueryTerm t : qt) {
            // Find the location of this term in the criteria
            StringBuilder predicate = new StringBuilder();
            int start = 0;
            if (!t.getOperator().equals(JexlOperatorConstants.getOperator(ParserTreeConstants.JJTFUNCTIONNODE))) {
                predicate.append(fieldName).append(" ").append(t.getOperator()).append(" ").append(t.getValue());
                start = query.indexOf(predicate.toString());
            } else {
                predicate.append(t.getValue().toString());
                // need to find the second occurence of the string.
                start = query.indexOf(predicate.toString());
            }
            if (-1 == start) {
                log.warn("Unable to find predicate: " + predicate.toString() + " in rewritten criteria: " + query.toString());
            }
            int length = predicate.length();

            // Now modify the criteria to check the value of my.fieldName
            query.replace(start, start + length, newPredicate.toString());
        }

        if (log.isDebugEnabled()) {
            log.debug("leaving rewriteQuery with: " + query.toString());
        }
        return query;
    }

    /**
     * Evaluates the criteria against an event.
     *
     * @param eventFields
     */
    public boolean evaluate(Key topKey, EventFields eventFields) {

        this.modifiedQuery = null;
        boolean rewritten = false;

        // Copy the criteria
        StringBuilder q = new StringBuilder(query);
        // Copy the literals, we are going to remove elements from this set
        // when they are added to the JEXL context. This will allow us to
        // determine which items in the criteria where *NOT* in the data.
        HashSet<String> literalsCopy = new HashSet<String>(literals);

        // Loop through the event fields and add them to the JexlContext.
        for (Entry<String, Collection<FieldValue>> field : eventFields.asMap().entrySet()) {
            String fName = normalizeKey(topKey, field.getKey());
            fName = NodeToJexl.removeInvalidChars(fName);

            // If this field is not part of the expression, then skip it.
            if (!literals.contains(fName)) {
                continue;
            } else {
                literalsCopy.remove(fName);
            }

            // This field may have multiple values.
            if (field.getValue().size() == 0) {
                continue;
            } else if (field.getValue().size() == 1) {
                ctx.set(fName, new String(field.getValue().iterator().next().getValue()));
            } else {
                q = rewriteQuery(q, fName, field.getValue());
                rewritten = true;
            }// End of if

        }// End of loop

        // For any literals in the criteria that were not found in the data, add them to the context
        // with a null value.
        for (String lit : literalsCopy) {
            ctx.set(lit, null);
        }

        if (log.isDebugEnabled()) {
            log.debug("Evaluating criteria: " + q.toString());
        }

        this.modifiedQuery = q.toString();

        Boolean result = null;
        if (rewritten) {
            Script script = engine.createScript(this.modifiedQuery);
            try {
                result = (Boolean) script.execute(ctx);
            } catch (Exception e) {
                log.error("Error evaluating script: " + this.modifiedQuery + " against event" + eventFields.toString(), e);
            }
        } else {
            Expression expr = engine.createExpression(this.modifiedQuery);
            try {
                result = (Boolean) expr.evaluate(ctx);
            } catch (Exception e) {
                log.error("Error evaluating expression: " + this.modifiedQuery + " against event" + eventFields.toString(), e);
            }
        }
        if (null != result && result) {
            return true;
        } else {
            return false;
        }
    } // End of method

    public String normalizeKey(Key topKey, String fieldKey) {
      return fieldKey;
    }
}
