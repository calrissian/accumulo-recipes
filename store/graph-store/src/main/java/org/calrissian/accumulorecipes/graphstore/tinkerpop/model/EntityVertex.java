package org.calrissian.accumulorecipes.graphstore.tinkerpop.model;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.query.EntityVertexQuery;
import org.calrissian.mango.domain.Entity;


public class EntityVertex extends EntityElement implements Vertex {

  public EntityVertex(Entity entity, GraphStore graphStore, Auths auths) {
    super(entity, graphStore, auths);
  }

  @Override
  public Iterable<Edge> getEdges(Direction direction, String... strings) {
    return new EntityVertexQuery(this, graphStore, auths).direction(direction).labels(strings).edges();
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, String... strings) {
    return new EntityVertexQuery(this, graphStore, auths).direction(direction).labels(strings).vertices();
  }

  @Override
  public VertexQuery query() {
    return new EntityVertexQuery(this, graphStore, auths);
  }

  @Override
  public Edge addEdge(String s, Vertex vertex) {
    throw new UnsupportedOperationException("The calrissian entity graph is immutable. Use the EntityGraphStore to modify the graph");
  }
}
