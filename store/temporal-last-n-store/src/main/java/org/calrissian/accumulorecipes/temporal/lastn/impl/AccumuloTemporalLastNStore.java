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
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.commons.iterators.FirstNEntriesInRowIterator;
import org.calrissian.accumulorecipes.temporal.lastn.iterators.EventGroupingIterator;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.temporal.lastn.TemporalLastNStore;
import org.calrissian.accumulorecipes.temporal.lastn.support.MergeJoinIterable;
import org.calrissian.mango.accumulo.Scanners;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.io.IOException;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.FirstNEntriesInRowIterator.decodeRow;
import static org.calrissian.accumulorecipes.commons.support.TimestampUtil.generateTimestamp;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;

public class AccumuloTemporalLastNStore implements TemporalLastNStore {

    private static final String DEFAULT_TABLE_NAME = "temporalLastN";

    private static final String GROUP_DELIM = "____";
    public static final String DELIM = "\0";

    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 100000L, 10000L, 3);
    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;
    private final TypeRegistry<String> typeRegistry;


    public AccumuloTemporalLastNStore(Connector connector) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloTemporalLastNStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.connector = connector;
        this.tableName = tableName;
        this.typeRegistry = ACCUMULO_TYPES; //TODO allow caller to pass in types.

        if(!connector.tableOperations().exists(this.tableName))
            connector.tableOperations().create(this.tableName, false);

        this.writer = this.connector.createBatchWriter(this.tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    @Override
    public void put(String group, StoreEntry entry) {
        try {
            for(Tuple tuple : entry.getTuples()) {

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

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String buildEventValue(String id, long timestamp, Tuple tuple) throws TypeEncodingException {

        String[] fields = new String[] {
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
    public CloseableIterable<StoreEntry> get(Date start, Date stop, Collection<String> groups, int n, Auths auths) {

        List<Iterable<StoreEntry>> cursors = new LinkedList<Iterable<StoreEntry>>();
        String stopDay = generateTimestamp(start.getTime(), MetricTimeUnit.DAYS);
        String startDay = generateTimestamp(stop.getTime(), MetricTimeUnit.DAYS);

        String stopMinute = generateTimestamp(start.getTime(), MetricTimeUnit.MINUTES);
        String startMinute = generateTimestamp(stop.getTime(), MetricTimeUnit.MINUTES);

        for(String group : groups) {

            Key startKey = new Key(group + GROUP_DELIM + startDay, startMinute);
            Key stopKey = new Key(group + GROUP_DELIM + stopDay, stopMinute + "\uffff");

            try {
                BatchScanner scanner = connector.createBatchScanner(tableName, auths.getAuths(), 1);
                scanner.setRanges(singletonList(new Range(startKey, stopKey)));

                IteratorSetting setting = new IteratorSetting(7, EventGroupingIterator.class);
                scanner.addScanIterator(setting);

                IteratorSetting setting2 = new IteratorSetting(15, FirstNEntriesInRowIterator.class);
                FirstNEntriesInRowIterator.setNumKeysToReturn(setting2, n);
                scanner.addScanIterator(setting2);

                for(Map.Entry<Key,Value> entry : scanner) {
                    List<Map.Entry<Key, Value>> topEntries = decodeRow(entry.getKey(), entry.getValue());
                    Iterable<List<Map.Entry<Key,Value>>> entries = Iterables.transform(topEntries, rowDecodeXform);
                    cursors.add(Iterables.transform(entries, entryXform));
                }

                scanner.close();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
        return wrap(new MergeJoinIterable(cursors));
    }

    Function<List<Map.Entry<Key,Value>>, StoreEntry> entryXform = new Function<List<Map.Entry<Key, Value>>, StoreEntry>() {
        @Override
        public StoreEntry apply(List<Map.Entry<Key, Value>> entries) {
            StoreEntry toReturn = null;
            try {
                for(Map.Entry<Key,Value> tupleCol : entries) {
                    String[] splits = splitPreserveAllTokens(new String(tupleCol.getValue().get()), DELIM);
                    if(toReturn == null) {
                        toReturn = new StoreEntry(splits[0], Long.parseLong(splits[1]));
                    }
                    toReturn.put(new Tuple(splits[2], typeRegistry.decode(splits[3], splits[4]), splits[5]));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return toReturn;
        }
    };

    Function<Map.Entry<Key,Value>, List<Map.Entry<Key,Value>>> rowDecodeXform =
            new Function<Map.Entry<Key, Value>, List<Map.Entry<Key,Value>>>() {
        @Override
        public List<Map.Entry<Key, Value>> apply(Map.Entry<Key, Value> keyValueEntry) {
            try {
                return EventGroupingIterator.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

}
