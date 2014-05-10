package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.mango.domain.Entity;

import java.util.Set;

/**
 * Created by cjnolet on 5/10/14.
 */
public class EntityVertex extends EntityElement implements Vertex {

  public EntityVertex(Entity entity, GraphStore graphStore) {
    super(entity, graphStore);
  }

  @Override
  public Iterable<Edge> getEdges(Direction direction, String... strings) {
    return null;
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, String... strings) {
    return null;
  }

  @Override
  public VertexQuery query() {
    return null;
  }

  @Override
  public Edge addEdge(String s, Vertex vertex) {
    return null;
  }
}
