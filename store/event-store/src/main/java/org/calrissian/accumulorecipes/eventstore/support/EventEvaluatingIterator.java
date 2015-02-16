/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.eventstore.support;

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.FI_TYPE_KEY_SEP;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ParseException;
import org.calrissian.accumulorecipes.commons.iterators.EvaluatingIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.QueryEvaluator;

public class EventEvaluatingIterator extends EvaluatingIterator {

  @Override
  public QueryEvaluator getQueryEvaluator(String expression) throws ParseException {
    return new EventQueryEvaluator(expression);
  }



  protected static class EventQueryEvaluator extends QueryEvaluator {

    public EventQueryEvaluator(String query) throws ParseException {
      super(query);
    }

    @Override
    public String normalizeKey(Key topKey, String fieldKey) {
      String cf = topKey.getColumnFamily().toString();
      String rewrittenKey =  splitPreserveAllTokens(cf, ONE_BYTE)[1] + FI_TYPE_KEY_SEP + fieldKey;

      return rewrittenKey;
    }
  }
}
