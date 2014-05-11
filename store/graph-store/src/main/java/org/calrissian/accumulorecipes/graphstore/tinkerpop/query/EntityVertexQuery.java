package org.calrissian.accumulorecipes.graphstore.tinkerpop.query;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.criteria.builder.QueryBuilder;

import java.util.Collection;
import java.util.List;

import static com.tinkerpop.blueprints.Query.Compare.*;
import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.graphstore.tinkerpop.BlueprintsGraphStore.*;
import static org.calrissian.mango.collect.CloseableIterables.*;

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
  public VertexQuery labels(String... strings) {
    this.labels = strings;
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
  public VertexQuery has(String s) {
    queryBuilder = queryBuilder.has(s);
    return this;
  }

  @Override
  public VertexQuery hasNot(String s) {
    queryBuilder = queryBuilder.hasNot(s);
    return this;
  }

  @Override
  public VertexQuery has(String s, Object o) {
    queryBuilder = queryBuilder.eq(s, o);
    return this;
  }

  @Override
  public VertexQuery hasNot(String s, Object o) {
    queryBuilder = queryBuilder.notEq(s, o);
    return this;
  }

  @Override
  public VertexQuery has(String s, Predicate predicate, Object o) {
    if(predicate == EQUAL)
      return has(s, o);
    else if(predicate == NOT_EQUAL)
      return hasNot(s, o);
    else if(predicate == GREATER_THAN)
      queryBuilder = queryBuilder.greaterThan(s, o);
    else if(predicate == LESS_THAN)
      queryBuilder = queryBuilder.lessThan(s, o);
    else if(predicate == GREATER_THAN_EQUAL)
      queryBuilder = queryBuilder.greaterThanEq(s, o);
    else if(predicate == LESS_THAN_EQUAL)
      queryBuilder = queryBuilder.lessThanEq(s, o);
    else
      throw new UnsupportedOperationException("Predicate with type " + predicate + " is not supported.");

    return this;
  }

  @Override
  public <T extends Comparable<T>> VertexQuery has(String s, T t, Compare compare) {
    return has(s, compare, t);
  }

  @Override
  public <T extends Comparable<?>> VertexQuery interval(String s, T t, T t2) {
    queryBuilder = queryBuilder.range(s, t, t2);
    return this;
  }

  @Override
  public VertexQuery limit(int i) {
    limit = i;
    return this;
  }

  @Override
  public CloseableIterable<Edge> edges() {

    Collection<EntityIndex> vertexIndex = singleton(new EntityIndex(vertex.getEntity()));
    org.calrissian.accumulorecipes.graphstore.model.Direction dir =
            org.calrissian.accumulorecipes.graphstore.model.Direction.valueOf(direction.toString());
    CloseableIterable<EdgeEntity> entityEdgies = graphStore.adjacentEdges(vertexIndex, queryBuilder.build(), dir, auths);
    CloseableIterable<Edge> finalEdges = transform(entityEdgies, new EdgeEntityXform(graphStore, auths));

    if(limit > -1)
      return CloseableIterables.limit(finalEdges, limit);
    return finalEdges;
  }

  @Override
  public CloseableIterable<Vertex> vertices() {

    CloseableIterable<Edge> edges = edges();

    CloseableIterable<Vertex> vertices = concat(transform(partition(edges, 50),
            new Function<List<Edge>, CloseableIterable<Vertex>>() {
      @Override
      public CloseableIterable<Vertex> apply(List<Edge> edges) {
        Iterable<EntityIndex> indexes = Iterables.transform(edges, new EntityIndexXform());
        return transform(graphStore.get(indexes, null, auths), new VertexEntityXform(graphStore,auths));
      }

    }));

    return vertices;
  }
}
