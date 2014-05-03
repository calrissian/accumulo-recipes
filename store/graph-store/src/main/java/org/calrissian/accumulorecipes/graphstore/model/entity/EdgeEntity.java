package org.calrissian.accumulorecipes.graphstore.model.entity;

import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.graphstore.model.Edge;

public class EdgeEntity extends Entity implements Edge {

  public EdgeEntity(String type, String id) {
    super(type, id);
  }
}
