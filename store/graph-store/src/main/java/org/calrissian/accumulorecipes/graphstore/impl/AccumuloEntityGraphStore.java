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
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.support.EdgeGroupingIterator;
import org.calrissian.accumulorecipes.graphstore.support.TupleStoreCriteriaPredicate;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.EntityGraph;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityRelationship;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.setVisibility;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.IN;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.OUT;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.*;
import static org.calrissian.mango.collect.CloseableIterables.*;
import static org.calrissian.mango.criteria.support.NodeUtils.criteriaFromNode;
import static org.calrissian.mango.types.encoders.AliasConstants.ENTITY_RELATIONSHIP_ALIAS;

/**
 * The AccumuloEntityGraphStore wraps an {@link AccumuloEntityStore} to provide an extra index which is capable
 * of making edges very fast to traverse for a possibly large set of input vertices. This graph store supports
 * breadth-first traversals.
 */
public class AccumuloEntityGraphStore extends AccumuloEntityStore implements GraphStore {

    public static final String DEFAULT_TABLE_NAME = "entityStore_graph";

    public static final int DEFAULT_BUFFER_SIZE = 50;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    public static final String ONE_BYTE = "\u0001";
    /**
     * Extracts an edge/vertex (depending on what is requested) on the far side of a given vertex
     */
    private Function<Map.Entry<Key, Value>, Entity> edgeRowXform = new Function<Map.Entry<Key, Value>, Entity>() {

        @Override
        public Entity apply(Map.Entry<Key, Value> keyValueEntry) {

            String cq = keyValueEntry.getKey().getColumnQualifier().toString();

            int idx = cq.indexOf(DELIM);
            String edge = cq.substring(0, idx);

            try {
                EntityRelationship edgeRel = (EntityRelationship) typeRegistry.decode(ENTITY_RELATIONSHIP_ALIAS, edge);
                Entity entity = new BaseEntity(edgeRel.getType(), edgeRel.getId());
                SortedMap<Key, Value> entries = EdgeGroupingIterator.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());

                for (Map.Entry<Key, Value> entry : entries.entrySet()) {
                    if (entry.getKey().getColumnQualifier().toString().indexOf(ONE_BYTE) == -1)
                        continue;

                    String[] qualParts = splitPreserveAllTokens(entry.getKey().getColumnQualifier().toString(), ONE_BYTE);
                    String[] keyALiasValue = splitPreserveAllTokens(qualParts[1], DELIM);

                    String vis = entry.getKey().getColumnVisibility().toString();
                    Tuple tuple = new Tuple(keyALiasValue[0], typeRegistry.decode(keyALiasValue[1], keyALiasValue[2]));
                    if(!vis.equals(""))
                        setVisibility(tuple, vis);
                    entity.put(tuple);

                }

                return entity;
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
        table = DEFAULT_TABLE_NAME;
        init();
    }

    public AccumuloEntityGraphStore(Connector connector, String indexTable, String shardTable, String edgeTable, EntityShardBuilder shardBuilder, StoreConfig config)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, shardBuilder, config);
        table = edgeTable;
        init();
    }

    private void init() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (!getConnector().tableOperations().exists(table))
            getConnector().tableOperations().create(table);

        writer = getConnector().createBatchWriter(table, getConfig().getMaxMemory(), getConfig().getMaxLatency(),
                getConfig().getMaxWriteThreads());
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIndex> fromVertices,
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
    public CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIndex> fromVertices, Node query, Direction direction, final Auths auths) {
        final CloseableIterable<Entity> entities = findAdjacentEdges(fromVertices, query, direction, null, auths);
        return transform(entities, new Function<Entity, EdgeEntity>() {
            @Override
            public EdgeEntity apply(Entity entity) {
                return new EdgeEntity(entity);
            }
        });
    }

    @Override
    public CloseableIterable<Entity> adjacencies(List<EntityIndex> fromVertices,
                                                 Node query, Direction direction,
                                                 Set<String> labels,
                                                 final Auths auths) {
        CloseableIterable<Entity> edges = findAdjacentEdges(fromVertices, query, direction, labels, auths);
        CloseableIterable<EntityIndex> indexes = transform(edges, new EntityGraph.EdgeToVertexIndexXform(direction));
        return concat(transform(partition(indexes, bufferSize),
                new Function<List<EntityIndex>, Iterable<Entity>>() {
                    @Override
                    public Iterable<Entity> apply(List<EntityIndex> entityIndexes) {
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
    public CloseableIterable<Entity> adjacencies(List<EntityIndex> fromVertices, Node query, Direction direction, final Auths auths) {
        return adjacencies(fromVertices, query, direction, null, auths);
    }

    private CloseableIterable<Entity> findAdjacentEdges(List<EntityIndex> fromVertices,
                                                        Node query, Direction direction,
                                                        Set<String> labels,
                                                        final Auths auths) {
        checkNotNull(fromVertices);
        checkNotNull(auths);

        TupleStoreCriteriaPredicate filter =
                query != null ? new TupleStoreCriteriaPredicate(criteriaFromNode(query)) : null;

        // this one is fairly easy- return the adjacent edges that match the given query
        try {
            BatchScanner scanner = getConnector().createBatchScanner(table, auths.getAuths(), getConfig().getMaxQueryThreads());
            IteratorSetting setting = new IteratorSetting(15, EdgeGroupingIterator.class);
            scanner.addScanIterator(setting);

            Collection<Range> ranges = new ArrayList<Range>();
            for (EntityIndex entity : fromVertices) {
                String row = typeRegistry.encode(new EntityRelationship(entity.getType(), entity.getId()));
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
        ranges.add(prefix(row, direction.toString() + DELIM + defaultString(label)));
    }

    @Override
    public void save(Iterable<? extends Entity> entities) {
        super.save(entities);

        // here is where we want to store the edge index everytime an entity is saved
        for (Entity entity : entities) {
            if (isEdge(entity)) {

                EntityRelationship edgeRelationship = new EntityRelationship(entity);
                EntityRelationship toVertex = entity.<EntityRelationship>get(TAIL).getValue();
                EntityRelationship fromVertex = entity.<EntityRelationship>get(HEAD).getValue();

                String toVertexVis = getVisibility(entity.get(TAIL), "");
                String fromVertexVis = getVisibility(entity.get(HEAD), "");

                String label = entity.<String>get(LABEL).getValue();

                try {
                    String fromEncoded = typeRegistry.encode(fromVertex);
                    String toEncoded = typeRegistry.encode(toVertex);
                    String edgeEncoded = typeRegistry.encode(edgeRelationship);
                    Mutation forward = new Mutation(fromEncoded);
                    Mutation reverse = new Mutation(toEncoded);

                    forward.put(new Text(OUT.toString() + DELIM + label), new Text(edgeEncoded + DELIM + toEncoded),
                            new ColumnVisibility(toVertexVis), EMPTY_VALUE);

                    reverse.put(new Text(IN.toString() + DELIM + label), new Text(edgeEncoded + DELIM + fromEncoded),
                            new ColumnVisibility(fromVertexVis), EMPTY_VALUE);

                    for (Tuple tuple : entity.getTuples()) {
                        String key = tuple.getKey();
                        String alias = typeRegistry.getAlias(tuple.getValue());
                        String value = typeRegistry.encode(tuple.getValue());

                        String keyAliasValue = key + DELIM + alias + DELIM + value;

                        forward.put(new Text(OUT.toString() + DELIM + label),
                                new Text(edgeEncoded + DELIM + toEncoded + ONE_BYTE + keyAliasValue),
                                new ColumnVisibility(getVisibility(tuple, "")), EMPTY_VALUE);

                        reverse.put(new Text(IN.toString() + DELIM + label),
                                new Text(edgeEncoded + DELIM + fromEncoded + ONE_BYTE + keyAliasValue),
                                new ColumnVisibility(getVisibility(tuple, "")), EMPTY_VALUE);
                    }

                    writer.addMutation(forward);
                    writer.addMutation(reverse);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        try {
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
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
}
