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
package org.calrissian.accumulorecipes.eventstore.support;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;
import org.calrissian.accumulorecipes.eventstore.iterator.EventIntersectingIterator;
import org.calrissian.accumulorecipes.eventstore.iterator.EventIterator;
import org.calrissian.accumulorecipes.eventstore.support.query.validators.AndSingleDepthOnlyValidator;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.EqualsLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.types.TypeContext;
import org.calrissian.mango.types.serialization.TupleModule;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;

import static java.util.Collections.singleton;
import static java.util.Map.Entry;
import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;

public class QueryNodeHelper {

    private final TypeContext typeContext;
    private final ObjectMapper objectMapper;
    private Function<Entry<Key, Value>, StoreEntry> entityTransform = new Function<Entry<Key, Value>, StoreEntry>() {
        @Override
        public StoreEntry apply(Entry<Key, Value> entry) {
            try {
                return objectMapper.readValue(entry.getValue().get(), StoreEntry.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    protected final Connector connector;
    protected final String shardTable;
    protected final Integer numThreads;
    protected final Shard shard;

    public QueryNodeHelper(Connector connector, String shardTable, int numThreads, Shard shard, TypeContext typeContext) {
        this.connector = connector;
        this.shardTable = shardTable;
        this.numThreads = numThreads;
        this.shard = shard;
        this.typeContext = typeContext;
        this.objectMapper = new ObjectMapper().withModule(new TupleModule(typeContext));
    }

    public CloseableIterable<StoreEntry> queryAndNode(Date start, Date stop, AndNode query, Authorizations auths)
            throws Exception {

        BatchScanner scanner = connector.createBatchScanner(shardTable, auths, numThreads);

        String[] range = shard.getRange(start, stop);

        IteratorSetting setting = new IteratorSetting(15, "timeLimit", TimeLimitingFilter.class);
        TimeLimitingFilter.setCurrentTime(setting, stop.getTime());
        TimeLimitingFilter.setTTL(setting, stop.getTime() - start.getTime());
        scanner.addScanIterator(setting);

        if (query != null && query.children() != null && query.children().size() > 1) {

            query.accept(new AndSingleDepthOnlyValidator());

            AndNodeColumns andNodeColumns = new AndNodeColumns(query, typeContext);

            IteratorSetting is = new IteratorSetting(16, "eventIntersectingIterator", EventIntersectingIterator.class);
            EventIntersectingIterator.setColumnFamilies(is, andNodeColumns.getColumns(), andNodeColumns.getNotFlags());
            scanner.addScanIterator(is);

        } else {
            throw new IllegalArgumentException("You must have 2 or more items to query.");
        }

        scanner.setRanges(singleton(new Range(range[0], range[1] + DELIM_END)));

        return transform(wrap(scanner), entityTransform);
    }


    public CloseableIterable<StoreEntry> querySingleLeaf(Date start, Date stop, Leaf query, Authorizations auths) throws Exception {

        BatchScanner scanner = connector.createBatchScanner(shardTable, auths, numThreads);

        String[] range = shard.getRange(start,  stop);

        IteratorSetting setting = new IteratorSetting(15, "timeLimit", TimeLimitingFilter.class);
        TimeLimitingFilter.setCurrentTime(setting, stop.getTime());
        TimeLimitingFilter.setTTL(setting, stop.getTime() - start.getTime());
        scanner.addScanIterator(setting);

        if (query != null) {

            if (query instanceof EqualsLeaf) {
                EqualsLeaf equalsLeaf = (EqualsLeaf) query;

                IteratorSetting iteratorSetting = new IteratorSetting(16, "eventIterator", EventIterator.class);
                scanner.addScanIterator(iteratorSetting);
                scanner.fetchColumnFamily(new Text(SHARD_PREFIX_B + DELIM + equalsLeaf.getKey() +
                        DELIM + typeContext.getAliasForType(equalsLeaf.getValue()) +
                        DELIM + typeContext.normalize(equalsLeaf.getValue())));
            } else {
                throw new IllegalArgumentException("The query " + query + " was not supported");
            }

        } else {
            throw new RuntimeException("Need to have a query and/or leaves of the query");
        }

        scanner.setRanges(singleton(new Range(range[0], range[1] + DELIM_END)));

        return transform(wrap(scanner), entityTransform);
    }
}
