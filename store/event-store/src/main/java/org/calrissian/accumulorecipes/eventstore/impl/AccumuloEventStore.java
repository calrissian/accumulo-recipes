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
package org.calrissian.accumulorecipes.eventstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.EventFieldsFilteringIterator;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.EventIndex;
import org.calrissian.accumulorecipes.eventstore.support.EventKeyValueIndex;
import org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper;
import org.calrissian.accumulorecipes.eventstore.support.shard.EventShardBuilder;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

/**
 * The Accumulo implementation of the EventStore which uses deterministic sharding to distribute ingest/queries over
 * the cloud to speed them up.
 */
public class AccumuloEventStore implements EventStore {

    public static final String DEFAULT_IDX_TABLE_NAME = "eventStore_index";
    public static final String DEFAULT_SHARD_TABLE_NAME = "eventStore_shard";

    public static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);
    public static final EventShardBuilder DEFAULT_SHARD_BUILDER = new HourlyShardBuilder(DEFAULT_PARTITION_SIZE);
    private final EventShardBuilder shardBuilder = DEFAULT_SHARD_BUILDER;
    private final EventQfdHelper helper;

    public AccumuloEventStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_STORE_CONFIG, LEXI_TYPES);
    }

    public AccumuloEventStore(Connector connector, String indexTable, String shardTable, StoreConfig config, TypeRegistry<String> typeRegistry) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector);
        checkNotNull(indexTable);
        checkNotNull(shardTable);
        checkNotNull(config);
        checkNotNull(typeRegistry);

        KeyValueIndex<Event> keyValueIndex = new EventKeyValueIndex(connector, indexTable, shardBuilder, config, typeRegistry);

        helper = new EventQfdHelper(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
    }

    /**
     * Free up any resources used by the store.
     *
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        helper.shutdown();
    }

    /**
     * Events get save into a sharded table to parallelize queries & ingest. Since the data is temporal by default,
     * an index table allows the lookup of events by UUID only (when the event's timestamp is not known).
     *
     * @param events
     * @throws Exception
     */
    @Override
    public void save(Iterable<? extends Event> events) {
        helper.save(events);
    }

    /**
     * Shard ids for which to scan are generated from the given start and end time. The given query specifies
     * which events to return. It is propagated all the way down to the iterators so that the query is run in parallel
     * on each tablet that needs to be scanned.
     */
    @Override
    public CloseableIterable<Event> query(Date start, Date end, Node query, final Set<String> selectFields, Auths auths) {
        checkNotNull(start);
        checkNotNull(end);
        checkNotNull(query);
        checkNotNull(auths);

        BatchScanner indexScanner = helper.buildIndexScanner(auths.getAuths());
        GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, indexScanner, shardBuilder);

        BatchScanner scanner = helper.buildShardScanner(auths.getAuths());
        IteratorSetting timeFilter = new IteratorSetting(7, TimeLimitingFilter.class);
        TimeLimitingFilter.setCurrentTime(timeFilter, end.getTime());
        TimeLimitingFilter.setTTL(timeFilter, end.getTime() - start.getTime());
        scanner.addScanIterator(timeFilter);

        CloseableIterable<Event> events = helper.query(scanner, globalIndexVisitor, query,
                helper.buildQueryXform(selectFields), auths);
        indexScanner.close();

        return events;
    }

    /**
     * This method will batch get a bunch of events by uuid (and optionally timestamp). If another store is used to
     * index into events in this store in a specially designed way (i.e. getting the last-n events, etc...) then
     * the most optimal way to store the index would be the UUID and the timestamp. However, if all that is known
     * about an event is the uuid, this method will do the extra fetch of the timestamp from the index table.
     */
    @Override
    public CloseableIterable<Event> get(Collection<EventIndex> uuids, Set<String> selectFields, Auths auths) {
        checkNotNull(uuids);
        checkNotNull(auths);
        try {

            BatchScanner scanner = helper.buildIndexScanner(auths.getAuths());

            Collection<Range> ranges = new LinkedList<Range>();
            for (EventIndex uuid : uuids) {
                if (uuid.getTimestamp() == null)
                    ranges.add(new Range(INDEX_V + "_string__" + uuid.getUuid()));
            }

            BatchScanner eventScanner = helper.buildShardScanner(auths.getAuths());
            Collection<Range> eventRanges = new LinkedList<Range>();
            if(ranges.size() > 0) {
                scanner.setRanges(ranges);
                scanner.fetchColumnFamily(new Text("@id"));

                Iterator<Map.Entry<Key, Value>> itr = scanner.iterator();

                /**
                 * Should just be one index for the shard containing the uuid
                 */
                while (itr.hasNext()) {
                    Map.Entry<Key, Value> entry = itr.next();
                    String shardId = entry.getKey().getColumnQualifier().toString();
                    String[] rowParts = splitByWholeSeparatorPreserveAllTokens(entry.getKey().getRow().toString(), "__");
                    eventRanges.add(Range.prefix(shardId, rowParts[1]));
                }

                scanner.close();
            }

            for (EventIndex curIndex : uuids) {
                if (curIndex.getTimestamp() != null) {
                    String shardId = shardBuilder.buildShard(new BaseEvent(curIndex.getUuid(), curIndex.getTimestamp()));
                    eventRanges.add(Range.prefix(shardId, curIndex.getUuid()));
                }
            }

            eventScanner.setRanges(eventRanges);

            IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
            eventScanner.addScanIterator(iteratorSetting);

            if (selectFields != null && selectFields.size() > 0) {
                iteratorSetting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
                EventFieldsFilteringIterator.setSelectFields(iteratorSetting, selectFields);
                eventScanner.addScanIterator(iteratorSetting);
            }

            return transform(closeableIterable(eventScanner), helper.buildWholeColFXform());
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
