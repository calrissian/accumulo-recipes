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
package org.calrissian.accumulorecipes.commons.iterators;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

public class EventFieldsFilteringIterator extends Filter {

    protected static final String SELECT_FIELDS = "selectFields";
    private Set<String> selectFields;

    public static void setSelectFields(IteratorSetting is, Set<String> selectFields) {
        is.addOption(SELECT_FIELDS, StringUtils.join(selectFields, NULL_BYTE));
    }

    @Override
    public boolean accept(Key k, Value v) {
        if (!k.getColumnFamily().toString().startsWith("fi")) {
            int ifx = k.getColumnQualifier().toString().indexOf(NULL_BYTE);
            String key = k.getColumnQualifier().toString().substring(0, ifx);
            return selectFields.contains(key);
        }
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options == null)
            throw new IllegalArgumentException(SELECT_FIELDS + " must be set for " + EventFieldsFilteringIterator.class.getName());

        String eventFieldsOpt = options.get(SELECT_FIELDS);
        if (eventFieldsOpt == null)
            throw new IllegalArgumentException(SELECT_FIELDS + " must be set for " + EventFieldsFilteringIterator.class.getName());

        selectFields = new HashSet<String>(asList(splitPreserveAllTokens(eventFieldsOpt, NULL_BYTE)));
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        EventFieldsFilteringIterator copy = (EventFieldsFilteringIterator) super.deepCopy(env);
        copy.selectFields = selectFields;
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(SELECT_FIELDS, "fields to allow through (delimited by \u0000)");
        io.setDescription("EventFieldsFIlteringIterator only allows fields through that have a key existing in a given selection set");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        super.validateOptions(options);
        if (!options.containsKey(SELECT_FIELDS))
            return false;
        return true;
    }

}
