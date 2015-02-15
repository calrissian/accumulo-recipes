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
package org.calrissian.accumulorecipes.commons.iterators;

import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.decodeRow;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.encodeRow;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.collections.map.LRUMap;

/**
 * For a keyValue containing multiple fieldName and fieldValue keys encoded into the value, this will filter out
 * only the keys which have visibilities matching a given set of Authorizations.
 */
public class VisibilityExtractor extends WrappingIterator {

    public static final String AUTHS = "auths";

    private VisibilityEvaluator checker;
    private LRUMap visibilityMap = new LRUMap();

    @Override public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        checker = new VisibilityEvaluator(new Authorizations(options.get(AUTHS).getBytes()));
    }

    public static void setAuthorizations(IteratorSetting setting, Authorizations authorizations) {
        setting.addOption(AUTHS, authorizations.serialize());
    }

    @Override
    public Value getTopValue() {

        Collection<Map.Entry<Key,Value>> keysValue = Lists.newArrayList();
        try {
            List<Map.Entry<Key,Value>> map = decodeRow(getTopKey(), super.getTopValue());
            for(Map.Entry<Key,Value> entry : map) {
                if(checker.evaluate(getColumnVisibility(entry.getKey()))) {
                    keysValue.add(Maps.immutableEntry(entry.getKey(), entry.getValue()));
                }
            }
            return encodeRow(keysValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (VisibilityParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param key
     * @return
     */
    public ColumnVisibility getColumnVisibility(Key key) {
      ColumnVisibility result = (ColumnVisibility) visibilityMap.get(key.getColumnVisibility());
      if (result != null)
        return result;
      result = new ColumnVisibility(key.getColumnVisibility().getBytes());
      visibilityMap.put(key.getColumnVisibility(), result);
      return result;
    }
}
