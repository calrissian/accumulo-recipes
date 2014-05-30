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
package org.calrissian.accumulorecipes.temporal.lastn.impl;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.collect.EventMergeJoinIterable;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.FirstNEntriesInRowIterator;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.temporal.lastn.TemporalLastNStore;
import org.calrissian.accumulorecipes.temporal.lastn.iterators.EventGroupingIterator;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.io.IOException;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.FirstNEntriesInRowIterator.decodeRow;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.mango.collect.CloseableIterables.wrap;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class AccumuloTemporalLastNStore implements TemporalLastNStore {

    public static final String DELIM = "\0";
    Function<List<Map.Entry<Key, Value>>, Event> entryXform = new Function<List<Map.Entry<Key, Value>>, Event>() {
        @Override
        public Event apply(List<Map.Entry<Key, Value>> entries) {
            Event toReturn = null;
            try {
                for (Map.Entry<Key, Value> tupleCol : entries) {
                    String[] splits = splitPreserveAllTokens(new String(tupleCol.getValue().get()), DELIM);
                    if (toReturn == null) {
                        toReturn = new BaseEvent(splits[0], Long.parseLong(splits[1]));
                    }
                    toReturn.put(new Tuple(splits[2], typeRegistry.decode(splits[3], splits[4]), splits[5]));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return toReturn;
        }
    };
    private static final String DEFAULT_TABLE_NAME = "temporalLastN";
    private static final String GROUP_DELIM = "____";
    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000L, 10000L, 3);
    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;
    private final TypeRegistry<String> typeRegistry;
    Function<Map.Entry<Key, Value>, List<Map.Entry<Key, Value>>> rowDecodeXform =
            new Function<Map.Entry<Key, Value>, List<Map.Entry<Key, Value>>>() {
                @Override
                public List<Map.Entry<Key, Value>> apply(Map.Entry<Key, Value> keyValueEntry) {
                    try {
                        return EventGroupingIterator.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };

    public AccumuloTemporalLastNStore(Connector connector) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloTemporalLastNStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.connector = connector;
        this.tableName = tableName;
        this.typeRegistry = LEXI_TYPES; //TODO allow caller to pass in types.

        if (!connector.tableOperations().exists(this.tableName))
            connector.tableOperations().create(this.tableName, false);

        this.writer = this.connector.createBatchWriter(this.tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    @Override
    public void put(String group, Event entry) {
        try {
            for (Tuple tuple : entry.getTuples()) {

                Mutation m = new Mutation(group + GROUP_DELIM + generateTimestamp(entry.getTimestamp(), MetricTimeUnit.DAYS));
                m.put(
                        new Text(generateTimestamp(entry.getTimestamp(), MetricTimeUnit.MINUTES)),
                        new Text(""),
                        new ColumnVisibility(tuple.getVisibility()),
                        entry.getTimestamp(),
                        new Value(buildEventValue(entry.getId(), entry.getTimestamp(), tuple).getBytes())
                );

                writer.addMutation(m);
            }

            writer.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String buildEventValue(String id, long timestamp, Tuple tuple) throws TypeEncodingException {

        String[] fields = new String[]{
                id,
                Long.toString(timestamp),
                tuple.getKey(),
                typeRegistry.getAlias(tuple.getValue()),
                typeRegistry.encode(tuple.getValue()),
                tuple.getVisibility()
        };

        return StringUtils.join(fields, DELIM);
    }

    @Override
    public CloseableIterable<Event> get(Date start, Date stop, Collection<String> groups, int n, Auths auths) {

        List<Iterable<Event>> cursors = new LinkedList<Iterable<Event>>();
        String stopDay = generateTimestamp(start.getTime(), MetricTimeUnit.DAYS);
        String startDay = generateTimestamp(stop.getTime(), MetricTimeUnit.DAYS);

        String stopMinute = generateTimestamp(start.getTime(), MetricTimeUnit.MINUTES);
        String startMinute = generateTimestamp(stop.getTime(), MetricTimeUnit.MINUTES);

        for (String group : groups) {

            Key startKey = new Key(group + GROUP_DELIM + startDay, startMinute);
            Key stopKey = new Key(group + GROUP_DELIM + stopDay, stopMinute + "\uffff");

            try {
                BatchScanner scanner = connector.createBatchScanner(tableName, auths.getAuths(), 1);
                scanner.setRanges(singletonList(new Range(startKey, stopKey)));

                IteratorSetting setting = new IteratorSetting(7, EventGroupingIterator.class);
                scanner.addScanIterator(setting);

                IteratorSetting timeFilter = new IteratorSetting(6, TimeLimitingFilter.class);
                TimeLimitingFilter.setCurrentTime(timeFilter, stop.getTime());
                TimeLimitingFilter.setTTL(timeFilter, stop.getTime() - start.getTime());
                scanner.addScanIterator(timeFilter);

                IteratorSetting setting2 = new IteratorSetting(15, FirstNEntriesInRowIterator.class);
                FirstNEntriesInRowIterator.setNumKeysToReturn(setting2, n);
                scanner.addScanIterator(setting2);

                for (Map.Entry<Key, Value> entry : scanner) {
                    List<Map.Entry<Key, Value>> topEntries = decodeRow(entry.getKey(), entry.getValue());
                    Iterable<List<Map.Entry<Key, Value>>> entries = Iterables.transform(topEntries, rowDecodeXform);
                    cursors.add(Iterables.transform(entries, entryXform));
                }

                scanner.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
        return wrap(new EventMergeJoinIterable(cursors));
    }

}
