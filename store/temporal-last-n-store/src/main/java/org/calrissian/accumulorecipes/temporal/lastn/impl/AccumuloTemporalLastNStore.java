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

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.collect.EventMergeJoinIterable;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.FirstNEntriesInRowIterator;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnQualifierIterator;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.temporal.lastn.TemporalLastNStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.encoders.lexi.LongReverseEncoder;

import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singletonList;
import static org.apache.accumulo.core.client.admin.TimeType.LOGICAL;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.RowEncoderUtil.decodeRow;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.setVisibility;
import static org.calrissian.mango.collect.CloseableIterables.wrap;
import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;

public class AccumuloTemporalLastNStore implements TemporalLastNStore {

    private static final String DEFAULT_TABLE_NAME = "temporalLastN";
    private static final String GROUP_DELIM = "____";
    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000L, 10000L, 3);
    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;
    private final TypeRegistry<String> typeRegistry;
    private static final LongReverseEncoder encoder = new LongReverseEncoder();

    private final Function<List<Map.Entry<Key, Value>>, Event> entryXform = new Function<List<Map.Entry<Key, Value>>, Event>() {
        @Override
        public Event apply(List<Map.Entry<Key, Value>> entries) {
            Event toReturn = null;

            for (Map.Entry<Key, Value> tupleCol : entries) {
                String[] splits = splitPreserveAllTokens(new String(tupleCol.getValue().get()), NULL_BYTE);
                String cq = tupleCol.getKey().getColumnQualifier().toString();
                int idx = cq.lastIndexOf(ONE_BYTE);
                if (toReturn == null)
                    toReturn = new BaseEvent(cq.substring(idx+1,cq.length()), encoder.decode(cq.substring(0, idx)));
                String vis = splits[3];
                toReturn.put(new Tuple(splits[0], typeRegistry.decode(splits[1], splits[2]), setVisibility(new HashMap<String, Object>(1), vis)));
            }

            return toReturn;
        }
    };

    Function<Map.Entry<Key, Value>, List<Map.Entry<Key, Value>>> rowDecodeXform =
        new Function<Map.Entry<Key, Value>, List<Map.Entry<Key, Value>>>() {
            @Override
            public List<Map.Entry<Key, Value>> apply(Map.Entry<Key, Value> keyValueEntry) {
                try {
                    return decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

    public AccumuloTemporalLastNStore(Connector connector)
        throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloTemporalLastNStore(Connector connector, String tableName, StoreConfig config)
        throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.connector = connector;
        this.tableName = tableName;
        this.typeRegistry = SIMPLE_TYPES;

        if (!connector.tableOperations().exists(this.tableName))
            connector.tableOperations().create(this.tableName, false, LOGICAL);

        this.writer = this.connector.createBatchWriter(
            this.tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    @Override
    public void put(String group, Event entry) {
        try {
            for (Tuple tuple : entry.getTuples()) {
                Mutation m = new Mutation(group + GROUP_DELIM + generateTimestamp(entry.getTimestamp(), TimeUnit.DAYS));
                m.put(
                    new Text(generateTimestamp(entry.getTimestamp(), TimeUnit.MINUTES)),
                    new Text(encoder.encode(entry.getTimestamp()) + ONE_BYTE + entry.getId()),
                    new ColumnVisibility(getVisibility(tuple, "")),
                    new Value(buildEventValue(tuple).getBytes())
                );
                writer.addMutation(m);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws Exception {
        writer.flush();
    }

    private String buildEventValue(Tuple tuple) {

        String[] fields = new String[]{
            tuple.getKey(),
            typeRegistry.getAlias(tuple.getValue()),
            typeRegistry.encode(tuple.getValue()),
            getVisibility(tuple, "")
        };

        return join(fields, NULL_BYTE);
    }

    @Override
    public CloseableIterable<Event> get(Date start, Date stop, Collection<String> groups, int n, Auths auths) {

        List<Iterable<Event>> cursors = new LinkedList<Iterable<Event>>();
        String stopDay = generateTimestamp(start.getTime(), TimeUnit.DAYS);
        String startDay = generateTimestamp(stop.getTime(), TimeUnit.DAYS);

        String stopMinute = generateTimestamp(start.getTime(), TimeUnit.MINUTES);
        String startMinute = generateTimestamp(stop.getTime(), TimeUnit.MINUTES);

        String stopMillis = encoder.encode(start.getTime());
        String startMillis = encoder.encode(stop.getTime());

        for (String group : groups) {

            Key startKey = new Key(group + GROUP_DELIM + startDay, startMinute, startMillis + ONE_BYTE);
            Key stopKey = new Key(group + GROUP_DELIM + stopDay, stopMinute, stopMillis + ONE_BYTE + END_BYTE);

            try {
                BatchScanner scanner = connector.createBatchScanner(tableName, auths.getAuths(), 1);
                scanner.setRanges(singletonList(new Range(startKey, stopKey)));

                IteratorSetting setting = new IteratorSetting(7, WholeColumnQualifierIterator.class);
                scanner.addScanIterator(setting);

                IteratorSetting setting2 = new IteratorSetting(15, FirstNEntriesInRowIterator.class);
                FirstNEntriesInRowIterator.setNumKeysToReturn(setting2, n);
                scanner.addScanIterator(setting2);

                for (Map.Entry<Key, Value> entry : scanner) {
                    List<Map.Entry<Key, Value>> topEntries = decodeRow(entry.getKey(), entry.getValue());
                    Iterable<List<Map.Entry<Key, Value>>> entries = transform(topEntries, rowDecodeXform);
                    cursors.add(transform(entries, entryXform));
                }

                scanner.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return wrap(new EventMergeJoinIterable(cursors));
    }
}
