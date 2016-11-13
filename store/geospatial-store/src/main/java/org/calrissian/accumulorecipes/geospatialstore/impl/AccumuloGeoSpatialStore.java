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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.apache.commons.lang.Validate.isTrue;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.setVisibility;
import static org.calrissian.accumulorecipes.commons.util.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.util.RowEncoderUtil;
import org.calrissian.accumulorecipes.geospatialstore.GeoSpatialStore;
import org.calrissian.accumulorecipes.geospatialstore.support.BoundingBoxFilter;
import org.calrissian.accumulorecipes.geospatialstore.support.PrefixedColumnQualifierIterator;
import org.calrissian.accumulorecipes.geospatialstore.support.QuadTreeHelper;
import org.calrissian.accumulorecipes.geospatialstore.support.QuadTreeScanRange;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.calrissian.mango.types.TypeRegistry;

public class AccumuloGeoSpatialStore implements GeoSpatialStore {

    private static final String DEFAULT_TABLE_NAME = "geoStore";
    public static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig();
    public static final double DEFAULT_MAX_PRECISION = .002;
    public static final int DEFAULT_NUM_PARTITIONS = 50;
    private static Function<Map.Entry<Key, Value>, Entity> xform = new Function<Map.Entry<Key, Value>, Entity>() {
        @Override
        public Entity apply(Map.Entry<Key, Value> keyValueEntry) {

            String cf = keyValueEntry.getKey().getColumnFamily().toString();
            EntityBuilder entry = null;
            try {
                List<Map.Entry<Key, Value>> map = RowEncoderUtil.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
                for (Map.Entry<Key, Value> curEntry : map) {
                    String[] cqParts = splitPreserveAllTokens(curEntry.getKey().getColumnQualifier().toString(), NULL_BYTE);
                    if(entry == null)
                        entry =EntityBuilder.create(cf, cqParts[0]);
                    String vis = curEntry.getKey().getColumnVisibility().toString();
                    Attribute attribute = new Attribute(cqParts[3], registry.decode(cqParts[4], cqParts[5]), setVisibility(new HashMap<String, String>(1), vis));
                    entry.attr(attribute);
                }
                return entry.build();
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
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG, DEFAULT_MAX_PRECISION, DEFAULT_NUM_PARTITIONS);
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

    protected String buildId(String type) {
        return type;
    }

    protected String buildKeyValue(String id, Attribute attribute, Point2D.Double location) {
        return id + NULL_BYTE + location.getX() + NULL_BYTE + location.getY() + NULL_BYTE + attribute.getKey() + NULL_BYTE + registry.getAlias(attribute.getValue()) + NULL_BYTE + registry.encode(attribute.getValue());
    }

    @Override
    public void put(Iterable<Entity> entries, Point2D.Double location) {
        for (Entity entry : entries) {

            int partition = abs(entry.getId().hashCode() % numPartitions);

            Mutation m = new Mutation(buildRow(partition, location));

            for (Attribute attribute : entry.getAttributes()) {
                try {
                    // put in the forward mutation
                    m.put(new Text(buildId(entry.getType())),
                            new Text(buildKeyValue(entry.getId(), attribute, location)),
                            new ColumnVisibility(getVisibility(attribute, "")),
                            new Value("".getBytes()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            try {

                if (m.size() > 0)
                    writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                throw new RuntimeException(e);
            }


        }
    }

    @Override
    public void flush() throws Exception {
        writer.flush();
    }

    @Override
    public CloseableIterable<Entity> get(Rectangle2D.Double location, Set<String> types, Auths auths) {
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
            for(String type : types)
                scanner.fetchColumnFamily(new Text(type));

            IteratorSetting setting = new IteratorSetting(7, PrefixedColumnQualifierIterator.class);
            scanner.addScanIterator(setting);

            setting = new IteratorSetting(6, BoundingBoxFilter.class);
            BoundingBoxFilter.setBoundingBox(setting, location);
            scanner.addScanIterator(setting);

            return transform(closeableIterable(scanner), xform);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder {
        private final Connector connector;
        private String tableName = DEFAULT_TABLE_NAME;
        private StoreConfig config = DEFAULT_STORE_CONFIG;
        private double maxPrecision = DEFAULT_MAX_PRECISION;
        private int numPartitions = DEFAULT_NUM_PARTITIONS;


        public Builder(Connector connector) {
            checkNotNull(connector);
            this.connector = connector;
        }

        public Builder setTableName(String tableName) {
            checkNotNull(tableName);
            this.tableName = tableName;
            return this;
        }

        public Builder setConfig(StoreConfig config) {
            checkNotNull(config);
            this.config = config;
            return this;
        }

        public Builder setMaxPrecision(double maxPrecision) {
            isTrue(maxPrecision > 0);
            this.maxPrecision = maxPrecision;
            return this;
        }

        public Builder setNumPartitions(int numPartitions) {
            isTrue(numPartitions > 0);
            this.numPartitions = numPartitions;
            return this;
        }

        public AccumuloGeoSpatialStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            return new AccumuloGeoSpatialStore(connector, tableName, config, maxPrecision,numPartitions);
        }
    }
}
