package org.calrissian.accumulorecipes.graphstore.tinkerpop.query;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityEdge;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Entity;

import java.util.Set;

import static com.tinkerpop.blueprints.Query.Compare.*;
import static org.calrissian.mango.collect.CloseableIterables.transform;

public class EntityGraphQuery implements GraphQuery {

  private GraphStore graphStore;
  private Set<String> vertexTypes;
  private Set<String> edgeTypes;
  private Auths auths;


  private int limit = -1;

  private QueryBuilder queryBuilder = new QueryBuilder().and();

  public EntityGraphQuery(GraphStore graphStore, Set<String> vertexTypes, Set<String> edgeTypes, Auths auths) {
    this.graphStore = graphStore;
    this.auths = auths;
    this.vertexTypes = vertexTypes;
    this.edgeTypes = edgeTypes;
  }

  @Override
  public GraphQuery has(String s) {
    queryBuilder = queryBuilder.has(s);
    return this;
  }

  @Override
  public GraphQuery hasNot(String s) {
    queryBuilder = queryBuilder.hasNot(s);
    return this;
  }

  @Override
  public GraphQuery has(String s, Object o) {
    queryBuilder = queryBuilder.eq(s, o);
    return this;
  }

  @Override
  public GraphQuery hasNot(String s, Object o) {
    queryBuilder = queryBuilder.notEq(s, o);
    return this;
  }

  @Override
  public GraphQuery has(String s, Predicate predicate, Object o) {
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
  public <T extends Comparable<T>> GraphQuery has(String s, T t, Compare compare) {
    return has(s, compare, t);
  }

  @Override
  public <T extends Comparable<?>> GraphQuery interval(String s, T t, T t2) {
    queryBuilder = queryBuilder.range(s, t, t2);
    return this;
  }

  @Override
  public GraphQuery limit(int i) {
    limit = i;
    return this;
  }

  @Override
  public Iterable<Edge> edges() {

    CloseableIterable<Entity> entities = graphStore.query(edgeTypes, queryBuilder.build(), null, auths);
    return transform(entities, new Function<Entity, Edge>() {
      @Override
      public Edge apply(Entity entity) {
        return new EntityEdge(entity, graphStore, auths);
      }
    });
  }

  @Override
  public Iterable<Vertex> vertices() {
    CloseableIterable<Entity> entities = graphStore.query(vertexTypes, queryBuilder.build(), null, auths);
    return transform(entities, new Function<Entity, Vertex>() {
      @Override
      public Vertex apply(Entity entity) {
        return new EntityVertex(entity, graphStore, auths);
      }
    });
  }
}
