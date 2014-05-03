package org.calrissian.accumulorecipes.graphstore.model.entity;

import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.graphstore.model.Vertex;

public class VertexEntity extends Entity implements Vertex {
  public VertexEntity(String type, String id) {
    super(type, id);
  }
}
