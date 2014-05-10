package org.calrissian.accumulorecipes.graphstore.tinkerpop;


import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.mango.domain.Entity;

public class EntityEdge extends EntityElement implements Edge{


  public EntityEdge(Entity entity, GraphStore graphStore) {
    super(entity, graphStore);
  }

  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    return null;
  }

  @Override
  public String getLabel() {
    return null;
  }
}
