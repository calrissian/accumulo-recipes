package org.calrissian.accumulorecipes.graphstore.tinkerpop.query;

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
import org.calrissian.mango.domain.Entity;

import java.util.Set;

import static com.tinkerpop.blueprints.Query.Compare.*;
import static org.calrissian.accumulorecipes.graphstore.tinkerpop.EntityGraph.*;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.criteria.utils.NodeUtils.criteriaFromNode;

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
  public GraphQuery has(String s) {
    queryBuilder = queryBuilder.has(s);
    return this;
  }

  @Override
  public GraphQuery hasNot(String s) {
    filters = filters.hasNot(s);
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
  public CloseableIterable<Edge> edges() {
    Node query = queryBuilder.end().build();
    Node filter = filters.end().build();

    CloseableIterable<Entity> entities = query.children().size() > 0 ?
            graphStore.query(edgeTypes, query, null, auths) :
            graphStore.getAllByType(edgeTypes, null, auths);
    CloseableIterable<Edge> edges = transform(entities, new EdgeEntityXform(graphStore, auths));

    if(filter.children().size() > 0)
      edges = CloseableIterables.filter(edges, new EntityFilterPredicate(criteriaFromNode(filter)));

    if(limit > -1)
      return CloseableIterables.limit(edges, limit);
    else
      return edges;

  }

  @Override
  public CloseableIterable<Vertex> vertices() {
    Node query = queryBuilder.end().build();
    Node filter = filters.end().build();

    CloseableIterable<Entity> entities = query.children().size() > 0 ?
            graphStore.query(vertexTypes, query, null, auths) :
            graphStore.getAllByType(vertexTypes, null, auths);
    CloseableIterable<Vertex> vertices = transform(entities, new VertexEntityXform(graphStore, auths));

    if(filter.children().size() > 0)
      vertices = CloseableIterables.filter(vertices, new EntityFilterPredicate(criteriaFromNode(filter)));

    if(limit > -1)
      return CloseableIterables.limit(vertices, limit);
    else
      return vertices;
  }
}
