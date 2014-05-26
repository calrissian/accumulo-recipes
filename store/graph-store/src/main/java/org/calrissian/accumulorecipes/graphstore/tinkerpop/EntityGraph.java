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
package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityEdge;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityElement;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.query.EntityGraphQuery;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.criteria.Criteria;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityRelationship;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singletonList;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.HEAD;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.TAIL;
import static org.calrissian.mango.collect.CloseableIterables.transform;

/**
 * This is a read-only implementation of the Tinkerpop Blueprints graph model.
 */
public class EntityGraph implements Graph {

    protected GraphStore graphStore;
    protected Set<String> vertexTypes;
    protected Set<String> edgeTypes;

    protected Auths auths;

    /**
     * The graph is restriced only to the given vertex types and edge types. This allows only a smaller portion of the
     * larger graph to be traversed as if those are the only types that exist. The given auths are applied directly
     * to every query so that the results are further restricted.
     */
    public EntityGraph(GraphStore graphStore, Set<String> vertexTypes, Set<String> edgeTypes, Auths auths) {
        this.graphStore = graphStore;
        this.vertexTypes = vertexTypes;
        this.edgeTypes = edgeTypes;
        this.auths = auths;
    }

    @Override
    public Features getFeatures() {
        Features features = new Features();
        features.supportsTransactions = false;
        features.supportsDuplicateEdges = false;
        features.supportsSelfLoops = true;
        features.supportsSerializableObjectProperty = true;
        features.supportsBooleanProperty = true;
        features.supportsDoubleProperty = true;
        features.supportsFloatProperty = true;
        features.supportsIntegerProperty = true;
        features.supportsPrimitiveArrayProperty = false;
        features.supportsUniformListProperty = false;
        features.supportsMixedListProperty = false;
        features.supportsLongProperty = true;
        features.supportsMapProperty = false;
        features.supportsStringProperty = true;
        features.ignoresSuppliedIds = false;
        features.isPersistent = true;
        features.isWrapper = true;
        features.supportsIndices = true;
        features.supportsVertexIndex = true;
        features.supportsEdgeIndex = true;
        features.supportsKeyIndices = true;
        features.supportsVertexKeyIndex = true;
        features.supportsEdgeKeyIndex = true;
        features.supportsEdgeIteration = true;
        features.supportsVertexIteration = true;
        features.supportsEdgeRetrieval = true;
        features.supportsVertexProperties = true;
        features.supportsEdgeProperties = true;
        features.supportsThreadedTransactions = false;
        features.supportsTransactions = false;

        return features;

    }

    /**
     * Adding a vertex is not allowed because the graph store is read-only
     */
    @Override
    public Vertex addVertex(Object o) {
        throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
    }

    /**
     * Retrieves the vertex with the given {@link EntityIndex}
     */
    @Override
    public Vertex getVertex(Object entityIndex) {
        checkArgument(entityIndex instanceof EntityIndex);
        if (vertexTypes.contains(((EntityIndex) entityIndex).getType())) ;
        CloseableIterable<Entity> entities = graphStore.get(singletonList((EntityIndex) entityIndex), null, auths);
        Iterator<Entity> itr = entities.iterator();
        if (itr.hasNext()) {
            EntityVertex toReturn = new EntityVertex(itr.next(), graphStore, auths);
            entities.closeQuietly();
            return toReturn;
        }
        return null;
    }


    /**
     * Removing a vertex is not allowed because the graph is read-only
     */
    @Override
    public void removeVertex(Vertex vertex) {
        throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
    }

    /**
     * Retrieves all the vertices with the given vertex types configured on the current instance
     */
    @Override
    public CloseableIterable<Vertex> getVertices() {
        CloseableIterable<Entity> entities = graphStore.getAllByType(vertexTypes, null, auths);
        return transform(entities, new VertexEntityXform(graphStore, auths));
    }

    /**
     * Retrieves the vertices with the given property key and value
     */
    @Override
    public CloseableIterable<Vertex> getVertices(String key, Object value) {
        CloseableIterable<Entity> entities = graphStore.query(vertexTypes, new QueryBuilder().eq(key, value).build(), null, auths);
        return transform(entities, new VertexEntityXform(graphStore, auths));
    }

    /**
     * Adding edges is not allowed because this store is read-only
     */
    @Override
    public Edge addEdge(Object o, Vertex vertex, Vertex vertex2, String s) {
        throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
    }

    /**
     * Retrieves the edge at the specified {@link EntityIndex}.
     *
     * @param entityIndex The entity index object (type and id) for which to find the edge
     */
    @Override
    public Edge getEdge(Object entityIndex) {
        checkArgument(entityIndex instanceof EntityIndex);
        if (edgeTypes.contains(((EntityIndex) entityIndex).getType())) ;
        CloseableIterable<Entity> entities = graphStore.get(singletonList((EntityIndex) entityIndex), null, auths);
        Iterator<Entity> itr = entities.iterator();
        if (itr.hasNext()) {
            EntityEdge toReturn = new EntityEdge(itr.next(), graphStore, auths);
            entities.closeQuietly();
            return toReturn;
        }
        return null;
    }

    /**
     * The store is read-only so edges cannot be removed.
     */
    @Override
    public void removeEdge(Edge edge) {
        throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
    }

    /**
     * Returns all edges with the entity edge types configured on this instance
     */
    @Override
    public CloseableIterable<Edge> getEdges() {
        CloseableIterable<Entity> entities = graphStore.getAllByType(edgeTypes, null, auths);
        return transform(entities, new EdgeEntityXform(graphStore, auths));
    }

    /**
     * Returns edges with the given property key and value.
     */
    @Override
    public CloseableIterable<Edge> getEdges(String key, Object value) {
        CloseableIterable<Entity> entities = graphStore.query(edgeTypes, new QueryBuilder().eq(key, value).build(), null, auths);
        return transform(entities, new EdgeEntityXform(graphStore, auths));
    }

    /**
     * Returns a graph query builder object
     */
    @Override
    public GraphQuery query() {
        return new EntityGraphQuery(graphStore, vertexTypes, edgeTypes, auths);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase() + "{" +
                "graphStore=" + graphStore +
                ", vertexTypes=" + vertexTypes +
                ", edgeTypes=" + edgeTypes +
                ", auths=" + auths +
                '}';
    }

    /**
     * A transform function to turn an {@link Entity} into an {@link EntityEdge}
     */
    public static class EdgeEntityXform implements Function<Entity, Edge> {

        private GraphStore graphStore;
        private Auths auths;

        public EdgeEntityXform(GraphStore graphStore, Auths auths) {
            this.graphStore = graphStore;
            this.auths = auths;
        }

        @Override
        public Edge apply(Entity entity) {
            return new EntityEdge(entity, graphStore, auths);
        }
    }

    /**
     * A transform function to turn an {@link Entity} into an {@link EntityVertex}.
     */
    public static class VertexEntityXform implements Function<Entity, Vertex> {

        private GraphStore graphStore;
        private Auths auths;

        public VertexEntityXform(GraphStore graphStore, Auths auths) {
            this.graphStore = graphStore;
            this.auths = auths;
        }

        @Override
        public Vertex apply(Entity entity) {
            return new EntityVertex(entity, graphStore, auths);
        }
    }

    /**
     * A transform function to turn an {@link EntityElement} into an {@link EntityIndex} so that it can be directly
     * applied to the {@link EntityStore}.
     */
    public static class EntityIndexXform implements Function<Element, EntityIndex> {
        @Override
        public EntityIndex apply(Element element) {
            return new EntityIndex(((EntityElement) element).getEntity());
        }
    }

    /**
     * This transform function will pull the vertex index from the correct side of given the edge based on the given
     * direction and will structure the result as an index to be used to query the entity store.
     */
    public static class EdgeToVertexIndexXform implements Function<Entity, EntityIndex> {

        private org.calrissian.accumulorecipes.graphstore.model.Direction direction;

        public EdgeToVertexIndexXform(org.calrissian.accumulorecipes.graphstore.model.Direction direction) {
            this.direction = direction;
        }

        @Override
        public EntityIndex apply(Entity element) {

            String headOrTail;
            if (direction == Direction.IN)
                headOrTail = HEAD;
            else
                headOrTail = TAIL;

            EntityRelationship rel = element.<EntityRelationship>get(headOrTail).getValue();
            return new EntityIndex(rel.getTargetType(), rel.getTargetId());
        }
    }

    public static class EntityFilterPredicate<T extends Element> implements com.google.common.base.Predicate<T> {

        private Criteria criteria;

        public EntityFilterPredicate(Criteria criteria) {
            this.criteria = criteria;
        }

        @Override
        public boolean apply(T element) {
            return criteria.apply(((EntityElement) element).getEntity());
        }
    }
}
