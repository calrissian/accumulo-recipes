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
package org.calrissian.accumulorecipes.graphstore.impl;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.util.RowEncoderUtil;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.support.AttributeStoreCriteriaPredicate;
import org.calrissian.accumulorecipes.graphstore.support.EdgeGroupingIterator;
import org.calrissian.accumulorecipes.graphstore.support.EdgeToVertexIndexXform;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.calrissian.mango.domain.entity.EntityIdentifier;
import org.calrissian.mango.types.TypeRegistry;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.setVisibility;
import static org.calrissian.accumulorecipes.commons.util.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.IN;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.OUT;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.*;
import static org.calrissian.mango.collect.CloseableIterables.*;
import static org.calrissian.mango.criteria.support.NodeUtils.criteriaFromNode;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.calrissian.mango.types.encoders.AliasConstants.ENTITY_IDENTIFIER_ALIAS;

/**
 * The AccumuloEntityGraphStore wraps an {@link AccumuloEntityStore} to provide an extra index which is capable
 * of making edges very fast to traverse for a possibly large set of input vertices. This graph store supports
 * breadth-first traversals.
 */
public class AccumuloEntityGraphStore extends AccumuloEntityStore implements GraphStore {

    public static final String DEFAULT_TABLE_NAME = "entityStore_graph";

    public static final int DEFAULT_BUFFER_SIZE = 50;
    private final int bufferSize;
    private final TypeRegistry<String> typeRegistry;
    /**
     * Extracts an edge/vertex (depending on what is requested) on the far side of a given vertex
     */
    private Function<Map.Entry<Key, Value>, Entity> edgeRowXform = new Function<Map.Entry<Key, Value>, Entity>() {

        @Override
        public Entity apply(Map.Entry<Key, Value> keyValueEntry) {

            String cq = keyValueEntry.getKey().getColumnQualifier().toString();

            int idx = cq.indexOf(NULL_BYTE);
            String edge = cq.substring(0, idx);

            try {
                EntityIdentifier edgeRel = (EntityIdentifier) typeRegistry.decode(ENTITY_IDENTIFIER_ALIAS, edge);
                EntityBuilder entity = EntityBuilder.create(edgeRel.getType(), edgeRel.getId());
                List<Map.Entry<Key, Value>> entries = RowEncoderUtil.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());

                for (Map.Entry<Key, Value> entry : entries) {
                    if (entry.getKey().getColumnQualifier().toString().indexOf(ONE_BYTE) == -1)
                        continue;

                    String[] qualParts = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), ONE_BYTE);
                    String[] keyALiasValue = splitPreserveAllTokens(qualParts[1], NULL_BYTE);

                    String vis = entry.getKey().getColumnVisibility().toString();
                    Attribute attribute = new Attribute(keyALiasValue[0], typeRegistry.decode(keyALiasValue[1], keyALiasValue[2]), setVisibility(new HashMap<String, String>(1), vis));
                    entity.attr(attribute);

                }

                return entity.build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };


    protected String table;
    protected BatchWriter writer;

    public AccumuloEntityGraphStore(Connector connector)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector);
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.table = DEFAULT_TABLE_NAME;
        this.typeRegistry = LEXI_TYPES;
        init();
    }

    public AccumuloEntityGraphStore(Connector connector, String indexTable, String shardTable, String edgeTable, EntityShardBuilder shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry, int bufferSize)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, shardBuilder, config, typeRegistry);
        this.bufferSize = bufferSize;
        this.table = edgeTable;
        this.typeRegistry = typeRegistry;
        init();
    }

    private void init() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (!getConnector().tableOperations().exists(table))
            getConnector().tableOperations().create(table);

        writer = getConnector().createBatchWriter(table, getConfig().getMaxMemory(), getConfig().getMaxLatency(),
                getConfig().getMaxWriteThreads());
    }

    @Override
    public CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIdentifier> fromVertices,
                                                       Node query,
                                                       Direction direction,
                                                       Set<String> labels,
                                                       final Auths auths) {
        checkNotNull(labels);
        final CloseableIterable<Entity> entities = findAdjacentEdges(fromVertices, query, direction, labels, auths);
        return transform(entities, new Function<Entity, EdgeEntity>() {
            @Override
            public EdgeEntity apply(Entity entity) {
                return new EdgeEntity(entity);
            }
        });
    }

    @Override
    public CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIdentifier> fromVertices, Direction direction, Set<String> labels, Auths auths) {
        return adjacentEdges(fromVertices, null, direction, labels, auths);
    }

    @Override
    public CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIdentifier> fromVertices, Node query, Direction direction, final Auths auths) {
        final CloseableIterable<Entity> entities = findAdjacentEdges(fromVertices, query, direction, null, auths);
        return transform(entities, new Function<Entity, EdgeEntity>() {
            @Override
            public EdgeEntity apply(Entity entity) {
                return new EdgeEntity(entity);
            }
        });
    }

    @Override
    public CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIdentifier> fromVertices, Direction direction, Auths auths) {
        return adjacentEdges(fromVertices, null, direction, auths);
    }

    @Override
    public CloseableIterable<Entity> adjacencies(List<EntityIdentifier> fromVertices,
                                                 Node query, Direction direction,
                                                 Set<String> labels,
                                                 final Auths auths) {
        CloseableIterable<Entity> edges = findAdjacentEdges(fromVertices, query, direction, labels, auths);
        CloseableIterable<EntityIdentifier> indexes = transform(edges, new EdgeToVertexIndexXform(direction));
        return concat(transform(partition(indexes, bufferSize),
                new Function<List<EntityIdentifier>, Iterable<Entity>>() {
                    @Override
                    public Iterable<Entity> apply(List<EntityIdentifier> entityIndexes) {
                        List<Entity> entityCollection = new LinkedList<Entity>();
                        CloseableIterable<Entity> entities = get(entityIndexes, null, auths);
                        Iterables.addAll(entityCollection, entities);
                        entities.closeQuietly();
                        return entityCollection;
                    }
                }
        ));
    }

    @Override
    public CloseableIterable<Entity> adjacencies(List<EntityIdentifier> fromVertices, Direction direction, Set<String> labels, Auths auths) {
        return adjacencies(fromVertices, null, direction, labels, auths);
    }

    @Override
    public CloseableIterable<Entity> adjacencies(List<EntityIdentifier> fromVertices, Node query, Direction direction, final Auths auths) {
        return adjacencies(fromVertices, query, direction, null, auths);
    }

    @Override
    public CloseableIterable<Entity> adjacencies(List<EntityIdentifier> fromVertices, Direction direction, Auths auths) {
        return adjacencies(fromVertices, null, direction, auths);
    }

    private CloseableIterable<Entity> findAdjacentEdges(List<EntityIdentifier> fromVertices,
                                                        Node query, Direction direction,
                                                        Set<String> labels,
                                                        final Auths auths) {
        checkNotNull(fromVertices);
        checkNotNull(auths);

        AttributeStoreCriteriaPredicate filter =
                query != null ? new AttributeStoreCriteriaPredicate(criteriaFromNode(query)) : null;

        // this one is fairly easy- return the adjacent edges that match the given query
        try {
            BatchScanner scanner = getConnector().createBatchScanner(table, auths.getAuths(), getConfig().getMaxQueryThreads());
            IteratorSetting setting = new IteratorSetting(15, EdgeGroupingIterator.class);
            scanner.addScanIterator(setting);

            Collection<Range> ranges = new ArrayList<Range>();
            for (EntityIdentifier entity : fromVertices) {
                String row = typeRegistry.encode(new EntityIdentifier(entity.getType(), entity.getId()));
                if (labels != null) {
                    for (String label : labels)
                        populateRange(ranges, row, direction, label);
                } else
                    populateRange(ranges, row, direction, null);
            }

            scanner.setRanges(ranges);

            /**
             * This partitions the initial Accumulo rows in the scanner into buffers of <bufferSize> so that the full entities
             * can be grabbed from the server in batches instead of one at a time.
             */
            CloseableIterable<Entity> entities = transform(closeableIterable(scanner), edgeRowXform);

            if (filter != null)
                return filter(entities, filter);
            else
                return entities;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void populateRange(Collection<Range> ranges, String row, Direction direction, String label) {
        ranges.add(prefix(row, direction.toString() + NULL_BYTE + defaultString(label)));
    }

    @Override
    public void save(Iterable<Entity> entities) {
        super.save(entities);

        // here is where we want to store the edge index everytime an entity is saved
        for (Entity entity : entities) {
            if (isEdge(entity)) {

                EntityIdentifier edgeRelationship = entity.getIdentifier();
                EntityIdentifier toVertex = entity.<EntityIdentifier>get(TAIL).getValue();
                EntityIdentifier fromVertex = entity.<EntityIdentifier>get(HEAD).getValue();

                String toVertexVis = getVisibility(entity.get(TAIL), "");
                String fromVertexVis = getVisibility(entity.get(HEAD), "");

                String label = entity.<String>get(LABEL).getValue();

                try {
                    String fromEncoded = typeRegistry.encode(fromVertex);
                    String toEncoded = typeRegistry.encode(toVertex);
                    String edgeEncoded = typeRegistry.encode(edgeRelationship);
                    Mutation forward = new Mutation(fromEncoded);
                    Mutation reverse = new Mutation(toEncoded);

                    forward.put(new Text(OUT.toString() + NULL_BYTE + label), new Text(edgeEncoded + NULL_BYTE + toEncoded),
                            new ColumnVisibility(toVertexVis), EMPTY_VALUE);

                    reverse.put(new Text(IN.toString() + NULL_BYTE + label), new Text(edgeEncoded + NULL_BYTE + fromEncoded),
                            new ColumnVisibility(fromVertexVis), EMPTY_VALUE);

                    for (Attribute attribute : entity.getAttributes()) {
                        String key = attribute.getKey();
                        String alias = typeRegistry.getAlias(attribute.getValue());
                        String value = typeRegistry.encode(attribute.getValue());

                        String keyAliasValue = key + NULL_BYTE + alias + NULL_BYTE + value;

                        forward.put(new Text(OUT.toString() + NULL_BYTE + label),
                                new Text(edgeEncoded + NULL_BYTE + toEncoded + ONE_BYTE + keyAliasValue),
                                new ColumnVisibility(getVisibility(attribute, "")), EMPTY_VALUE);

                        reverse.put(new Text(IN.toString() + NULL_BYTE + label),
                                new Text(edgeEncoded + NULL_BYTE + fromEncoded + ONE_BYTE + keyAliasValue),
                                new ColumnVisibility(getVisibility(attribute, "")), EMPTY_VALUE);
                    }

                    writer.addMutation(forward);
                    writer.addMutation(reverse);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void flush() throws Exception {
        writer.flush();
        super.flush();
    }

    @Override
    public void shutdown() throws MutationsRejectedException {
        super.shutdown();
        writer.close();
    }

    private boolean isEdge(Entity entity) {
        return entity.get(HEAD) != null &&
                entity.get(TAIL) != null &&
                entity.get(LABEL) != null;
    }

    private Connector getConnector() {
        return getHelper().getConnector();
    }

    private StoreConfig getConfig() {
        return getHelper().getConfig();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase() + "{" +
                "table='" + table + '\'' +
                ", bufferSize=" + bufferSize +
                ", writer=" + writer +
                '}';
    }

    public static class Builder {
        private final Connector connector;
        private String indexTable = DEFAULT_IDX_TABLE_NAME;
        private String shardTable = DEFAULT_SHARD_TABLE_NAME;
        private String edgeTable = DEFAULT_TABLE_NAME;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private StoreConfig storeConfig = DEFAULT_STORE_CONFIG;
        private TypeRegistry<String> typeRegistry = LEXI_TYPES;
        private EntityShardBuilder shardBuilder = DEFAULT_SHARD_BUILDER;

        public Builder(Connector connector) {
            checkNotNull(connector);
            this.connector = connector;
        }

        public Builder setIndexTable(String indexTable) {
            checkNotNull(indexTable);
            this.indexTable = indexTable;
            return this;
        }

        public Builder setShardTable(String shardTable) {
            checkNotNull(shardTable);
            this.shardTable = shardTable;
            return this;
        }

        public Builder setEdgeTable(String edgeTable) {
            checkNotNull(edgeTable);
            this.edgeTable = edgeTable;
            return this;
        }

        public Builder setStoreConfig(StoreConfig storeConfig) {
            checkNotNull(storeConfig);
            this.storeConfig = storeConfig;
            return this;
        }

        public Builder setTypeRegistry(TypeRegistry<String> typeRegistry) {
            checkNotNull(typeRegistry);
            this.typeRegistry = typeRegistry;
            return this;
        }

        public Builder setShardBuilder(EntityShardBuilder shardBuilder) {
            checkNotNull(shardBuilder);
            this.shardBuilder = shardBuilder;
            return this;
        }

        public AccumuloEntityGraphStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            return new AccumuloEntityGraphStore(connector, indexTable, shardTable, edgeTable, shardBuilder, storeConfig, typeRegistry, bufferSize);
        }
    }
}
