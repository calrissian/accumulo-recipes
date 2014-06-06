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
package org.calrissian.accumulorecipes.graphstore.tinkerpop.query;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.entity.Entity;

import java.util.Set;

import static org.calrissian.accumulorecipes.graphstore.tinkerpop.EntityGraph.*;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.criteria.support.NodeUtils.criteriaFromNode;

public class EntityGraphQuery implements GraphQuery {

    private GraphStore graphStore;
    private Set<String> vertexTypes;
    private Set<String> edgeTypes;
    private Auths auths;

    private int limit = -1;

    private QueryBuilder queryBuilder = new QueryBuilder().and();
    private QueryBuilder filters = new QueryBuilder().and();

    public EntityGraphQuery(GraphStore graphStore, Set<String> vertexTypes, Set<String> edgeTypes, Auths auths) {
        this.graphStore = graphStore;
        this.auths = auths;
        this.vertexTypes = vertexTypes;
        this.edgeTypes = edgeTypes;
    }

    @Override
    public GraphQuery has(String key) {
        queryBuilder = queryBuilder.has(key);
        return this;
    }

    @Override
    public GraphQuery hasNot(String key) {
        filters = filters.hasNot(key);
        return this;
    }

    @Override
    public GraphQuery has(String key, Object value) {
        queryBuilder = queryBuilder.eq(key, value);
        return this;
    }

    @Override
    public GraphQuery hasNot(String key, Object value) {
        queryBuilder = queryBuilder.notEq(key, value);
        return this;
    }

    @Override
    public GraphQuery has(String key, Predicate predicate, Object value) {
        if (predicate.toString().equals("EQUAL"))
            return has(key, value);
        else if (predicate.toString().equals("NOT_EQUAL"))
            return hasNot(key, value);
        else if (predicate.toString().equals("GREATER_THAN"))
            queryBuilder = queryBuilder.greaterThan(key, value);
        else if (predicate.toString().equals("LESS_THAN"))
            queryBuilder = queryBuilder.lessThan(key, value);
        else if (predicate.toString().equals("GREATER_THAN_EQUAL"))
            queryBuilder = queryBuilder.greaterThanEq(key, value);
        else if (predicate.toString().equals("LESS_THAN_EQUAL"))
            queryBuilder = queryBuilder.lessThanEq(key, value);
        else if (predicate.toString().equals("IN"))
            queryBuilder = queryBuilder.in(key, value);
        else if(predicate.toString().equals("NOT_IN"))
            queryBuilder = queryBuilder.notIn(key, value);
        else
            throw new UnsupportedOperationException("Predicate with type " + predicate + " is not supported.");

        return this;
    }

    @Override
    public <T extends Comparable<T>> GraphQuery has(String key, T value, Compare compare) {
        return has(key, compare, value);
    }

    @Override
    public <T extends Comparable<?>> GraphQuery interval(String key, T start, T stop) {
        queryBuilder = queryBuilder.range(key, start, stop);
        return this;
    }

    @Override
    public GraphQuery limit(int i) {
        limit = i;
        return this;
    }

    @Override
    public CloseableIterable<Edge> edges() {
        return queryElements(edgeTypes, new EdgeEntityXform(graphStore, auths));
    }

    @Override
    public CloseableIterable<Vertex> vertices() {
        return queryElements(vertexTypes, new VertexEntityXform(graphStore, auths));
    }

    private <T> CloseableIterable<T> queryElements(Set<String> elementTypes, Function<Entity, T> function) {

        Node query = queryBuilder.end().build();
        Node filter = filters.end().build();

        CloseableIterable<Entity> entities = query.children().size() > 0 ?
                graphStore.query(elementTypes, query, null, auths) :
                graphStore.getAllByType(elementTypes, null, auths);
        CloseableIterable<T> elements = transform(entities, function);

        if (filter.children().size() > 0)
            elements = CloseableIterables.filter(elements, new EntityFilterPredicate(criteriaFromNode(filter)));

        if (limit > -1)
            return CloseableIterables.limit(elements, limit);
        else
            return elements;
    }
}
