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

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Sets.newHashSet;
import static com.tinkerpop.blueprints.Query.Compare.*;
import static java.util.Collections.singletonList;
import static org.calrissian.accumulorecipes.graphstore.tinkerpop.EntityGraph.*;
import static org.calrissian.mango.collect.CloseableIterables.chain;
import static org.calrissian.mango.collect.CloseableIterables.transform;

/**
 * This builder class allows a set of vertices and/or edges to be queried matching the given criteria. This class
 * will try to perform the most optimal query given the input but complex predicates like custom comparables will
 * require filtering.
 */
public class EntityVertexQuery implements VertexQuery{

  private GraphStore graphStore;
  private EntityVertex vertex;
  private Auths auths;

  private Direction direction = Direction.BOTH;
  private String[] labels = null;
  private int limit = -1;

  private QueryBuilder queryBuilder = new QueryBuilder().and();

  public EntityVertexQuery(EntityVertex vertex, GraphStore graphStore, Auths auths) {
    this.vertex = vertex;
    this.graphStore = graphStore;
    this.auths = auths;
  }

  @Override
  public VertexQuery direction(Direction direction) {
    this.direction = direction;
    return this;
  }

  @Override
  public VertexQuery labels(String... labels) {
    this.labels = labels;
    return this;
  }

  @Override
  public long count() {
    CloseableIterable<Edge> edges = edges();
    long count = Iterables.size(edges);
    edges.closeQuietly();
    return count;
  }

  @Override
  public CloseableIterable<EntityIndex> vertexIds() {
    return transform(vertices(), new EntityIndexXform());
  }

  @Override
  public VertexQuery has(String key) {
    queryBuilder = queryBuilder.has(key);
    return this;
  }

  @Override
  public VertexQuery hasNot(String key) {
    queryBuilder = queryBuilder.hasNot(key);
    return this;
  }

  @Override
  public VertexQuery has(String key, Object value) {
    queryBuilder = queryBuilder.eq(key, value);
    return this;
  }

  @Override
  public VertexQuery hasNot(String key, Object value) {
    queryBuilder = queryBuilder.notEq(key, value);
    return this;
  }

  @Override
  public VertexQuery has(String key, Predicate predicate, Object value) {
    if(predicate == EQUAL)
      return has(key, value);
    else if(predicate == NOT_EQUAL)
      return hasNot(key, value);
    else if(predicate == GREATER_THAN)
      queryBuilder = queryBuilder.greaterThan(key, value);
    else if(predicate == LESS_THAN)
      queryBuilder = queryBuilder.lessThan(key, value);
    else if(predicate == GREATER_THAN_EQUAL)
      queryBuilder = queryBuilder.greaterThanEq(key, value);
    else if(predicate == LESS_THAN_EQUAL)
      queryBuilder = queryBuilder.lessThanEq(key, value);
    else
      throw new UnsupportedOperationException("Predicate with type " + predicate + " is not supported.");

    return this;
  }

  @Override
  public <T extends Comparable<T>> VertexQuery has(String key, T value, Compare compare) {
    return has(key, compare, value);
  }

  @Override
  public <T extends Comparable<?>> VertexQuery interval(String key, T start, T stop) {
    queryBuilder = queryBuilder.range(key, start, stop);
    return this;
  }

  @Override
  public VertexQuery limit(int limit) {
    this.limit = limit;
    return this;
  }

  @Override
  public CloseableIterable<Edge> edges() {

    Node query = queryBuilder.end().build();

    List<EntityIndex> vertexIndex = singletonList(new EntityIndex(vertex.getEntity()));

    List<org.calrissian.accumulorecipes.graphstore.model.Direction> dirs =
            new ArrayList<org.calrissian.accumulorecipes.graphstore.model.Direction>();

    if(direction == Direction.IN || direction == Direction.BOTH)
      dirs.add(org.calrissian.accumulorecipes.graphstore.model.Direction.IN);
    if(direction == Direction.OUT || direction == Direction.BOTH)
      dirs.add(org.calrissian.accumulorecipes.graphstore.model.Direction.OUT);

    CloseableIterable<? extends Entity> edges = null;
    for(org.calrissian.accumulorecipes.graphstore.model.Direction dir : dirs) {

      CloseableIterable<? extends Entity> entityEdges = labels == null ?
              graphStore.adjacentEdges(vertexIndex, query.children().size() > 0 ? query : null, dir, auths) :
              graphStore.adjacentEdges(vertexIndex, query.children().size() > 0 ? query : null, dir, newHashSet(labels), auths);

      if(edges == null)
        edges = entityEdges;
      else
        edges = chain(edges, entityEdges);

    }

    CloseableIterable<Edge> finalEdges = transform(edges, new EdgeEntityXform(graphStore, auths));

    if(limit > -1)
      return CloseableIterables.limit(finalEdges, limit);
    return finalEdges;
  }

  @Override
  public CloseableIterable<Vertex> vertices() {
    Node query = queryBuilder.end().build();

    List<EntityIndex> vertexIndex = singletonList(new EntityIndex(vertex.getEntity()));

    List<org.calrissian.accumulorecipes.graphstore.model.Direction> dirs =
            new ArrayList<org.calrissian.accumulorecipes.graphstore.model.Direction>();

    if(direction == Direction.IN || direction == Direction.BOTH)
      dirs.add(org.calrissian.accumulorecipes.graphstore.model.Direction.IN);
    if(direction == Direction.OUT || direction == Direction.BOTH)
      dirs.add(org.calrissian.accumulorecipes.graphstore.model.Direction.OUT);

    CloseableIterable<? extends Entity> vertices = null;
    for(org.calrissian.accumulorecipes.graphstore.model.Direction dir : dirs) {

      CloseableIterable<? extends Entity> entityvertices = labels == null ?
              graphStore.adjacencies(vertexIndex, query.children().size() > 0 ? query : null, dir, auths) :
              graphStore.adjacencies(vertexIndex, query.children().size() > 0 ? query : null, dir, newHashSet(labels), auths);

      if(vertices == null)
        vertices = entityvertices;
      else
        vertices = chain(vertices, entityvertices);

    }

    CloseableIterable<Vertex> finalVertices = transform(vertices, new VertexEntityXform(graphStore, auths));

    if(limit > -1)
      return CloseableIterables.limit(finalVertices, limit);
    return finalVertices;
  }
}
