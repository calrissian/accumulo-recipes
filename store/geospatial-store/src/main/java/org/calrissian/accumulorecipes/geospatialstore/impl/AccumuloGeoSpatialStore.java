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
package org.calrissian.accumulorecipes.geospatialstore.impl;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.geospatialstore.GeoSpatialStore;
import org.calrissian.accumulorecipes.geospatialstore.support.BoundingBoxFilter;
import org.calrissian.accumulorecipes.geospatialstore.support.QuadTreeHelper;
import org.calrissian.accumulorecipes.geospatialstore.support.QuadTreeScanRange;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.abs;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.setVisibility;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class AccumuloGeoSpatialStore implements GeoSpatialStore {

    private static final String DEFAULT_TABLE_NAME = "geoStore";
    private static Function<Map.Entry<Key, Value>, Event> xform = new Function<Map.Entry<Key, Value>, Event>() {
        @Override
        public Event apply(Map.Entry<Key, Value> keyValueEntry) {

            String[] cfParts = splitPreserveAllTokens(keyValueEntry.getKey().getColumnFamily().toString(), NULL_BYTE);
            Event entry = new BaseEvent(cfParts[0], Long.parseLong(cfParts[1]));
            try {
                Map<Key, Value> map = WholeColumnFamilyIterator.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
                for (Map.Entry<Key, Value> curEntry : map.entrySet()) {
                    String[] cqParts = splitPreserveAllTokens(curEntry.getKey().getColumnQualifier().toString(), NULL_BYTE);
                    String vis = curEntry.getKey().getColumnVisibility().toString();
                    Tuple tuple = new Tuple(cqParts[0], registry.decode(cqParts[1], cqParts[2]), setVisibility(new HashMap<String, Object>(1), vis));
                    entry.put(tuple);
                }
                return entry;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    private static final String PARTITION_DELIM = "_";
    private static final TypeRegistry registry = LEXI_TYPES;
    private final QuadTreeHelper helper = new QuadTreeHelper();
    private final BatchWriter writer;
    private final int numPartitions;
    private final double maxPrecision;
    private final Connector connector;
    private final StoreConfig config;
    private final String tableName;

    public AccumuloGeoSpatialStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_TABLE_NAME, new StoreConfig(), .002, 50);
    }

    public AccumuloGeoSpatialStore(Connector connector, String tableName, StoreConfig config, double maxPrecision, int numPartitions) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        this.connector = connector;
        this.config = config;

        this.tableName = tableName;
        this.maxPrecision = maxPrecision;
        this.numPartitions = numPartitions;


        if (!connector.tableOperations().exists(tableName))
            connector.tableOperations().create(tableName);

        writer = connector.createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    // for extensions
    protected Connector getConnector() {
        return connector;
    }

    protected int getPartitionWidth() {
        return Integer.toString(numPartitions).length();
    }

    protected String buildRow(int partition, Point2D.Double location) {
        return buildRow(partition, helper.buildGeohash(location, maxPrecision));
    }

    protected String buildRow(int partition, String hash) {
        return String.format("%0" + getPartitionWidth() + "d%s%s", partition, PARTITION_DELIM, hash);
    }

    protected String buildId(String id, long timestamp, Point2D.Double location) {
        return String.format("%s%s%s%s%s%s%s", id, NULL_BYTE, timestamp, NULL_BYTE, location.getX(), NULL_BYTE, location.getY());
    }

    protected String buildKeyValue(Tuple tuple) {
        return tuple.getKey() + NULL_BYTE + registry.getAlias(tuple.getValue()) + NULL_BYTE + registry.encode(tuple.getValue());
    }

    @Override
    public void put(Iterable<Event> entries, Point2D.Double location) {
        for (Event entry : entries) {

            int partition = abs(entry.getId().hashCode() % numPartitions);

            Mutation m = new Mutation(buildRow(partition, location));

            for (Tuple tuple : entry.getTuples()) {
                try {
                    // put in the forward mutation
                    m.put(new Text(buildId(entry.getId(), entry.getTimestamp(), location)),
                            new Text(buildKeyValue(tuple)),
                            new ColumnVisibility(getVisibility(tuple, "")),
                            entry.getTimestamp(),
                            new Value("".getBytes()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {

                if (m.size() > 0) {
                    writer.addMutation(m);
                    writer.flush();
                }
            } catch (MutationsRejectedException e) {
                throw new RuntimeException(e);
            }


        }
    }

    @Override
    public CloseableIterable<Event> get(Rectangle2D.Double location, Auths auths) {
        Collection<QuadTreeScanRange> ranges =
                helper.buildQueryRangesForBoundingBox(location, maxPrecision);

        try {
            BatchScanner scanner = connector.createBatchScanner(tableName, auths.getAuths(), config.getMaxQueryThreads());

            Collection<Range> theRanges = new ArrayList<Range>();
            for (QuadTreeScanRange range : ranges) {
                for (int i = 0; i < numPartitions; i++)
                    theRanges.add(new Range(buildRow(i, range.getMinimum()), buildRow(i, range.getMaximum())));
            }

            scanner.setRanges(theRanges);
            IteratorSetting setting = new IteratorSetting(7, WholeColumnFamilyIterator.class);
            scanner.addScanIterator(setting);

            setting = new IteratorSetting(6, BoundingBoxFilter.class);
            BoundingBoxFilter.setBoundingBox(setting, location);
            scanner.addScanIterator(setting);

            return transform(closeableIterable(scanner), xform);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
