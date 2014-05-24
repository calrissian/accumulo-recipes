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
package org.calrissian.accumulorecipes.eventstore.hadoop;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.shard.EventShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.Event;

import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.*;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.QueryXform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EventInputFormat extends BaseQfdInputFormat<Event, EventWritable> {

    public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
        setInputInfo(config, username, password, DEFAULT_SHARD_TABLE_NAME, auths);
    }

    public static void setQueryInfo(Configuration config, Date start, Date end, Node query, Set<String> selectFields) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        setQueryInfo(config, start, end, query, selectFields, DEFAULT_SHARD_BUILDER);
    }

    public static void setQueryInfo(Configuration config, Date start, Date end, Node query, Set<String> selectFields, EventShardBuilder shardBuilder) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        try {
            validateOptions(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Instance instance = getInstance(config);
        Connector connector = instance.getConnector(getUsername(config), getPassword(config));
        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getAuthorizations(config), 5);
        GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, scanner, shardBuilder);
        QueryOptimizer optimizer = new QueryOptimizer(query, globalIndexVisitor);
        NodeToJexl nodeToJexl = new NodeToJexl();
        String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());

        Collection<Range> ranges = new ArrayList<Range>();
        for (String shard : optimizer.getShards())
            ranges.add(new Range(shard));

        setRanges(config, ranges);

        IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
        setting.addOption(BooleanLogicIterator.QUERY_OPTION, jexl);
        setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

        addIterator(config, setting);
    }


    @Override
    protected Function<Map.Entry<Key, Value>, Event> getTransform(Configuration configuration) {
        final String[] selectFields = configuration.getStrings("selectFields");

        final Kryo kryo = new Kryo();
        initializeKryo(kryo);

        return new QueryXform(kryo, LEXI_TYPES, selectFields != null ? new HashSet<String>(asList(selectFields)) : null);
    }

    @Override
    protected EventWritable getWritable() {
        return new EventWritable();
    }
}
