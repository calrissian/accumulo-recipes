package org.calrissian.accumulorecipes.graphstore.impl;

import org.apache.accumulo.core.client.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.Edge;
import org.calrissian.accumulorecipes.graphstore.model.Vertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;

import java.util.Set;

import static org.calrissian.accumulorecipes.graphstore.model.Edge.HEAD;
import static org.calrissian.accumulorecipes.graphstore.model.Edge.LABEL;
import static org.calrissian.accumulorecipes.graphstore.model.Edge.TAIL;

public class AccumuloEntityGraphStore extends AccumuloEntityStore implements GraphStore {

  public AccumuloEntityGraphStore(Connector connector)
          throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    super(connector);
  }

  public AccumuloEntityGraphStore(Connector connector, String indexTable, String shardTable, StoreConfig config)
          throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    super(connector, indexTable, shardTable, config);
  }

  @Override
  public CloseableIterable<Edge> adjacentEdges(Iterable<Vertex> fromVertices,
                                               Node query,
                                               Direction direction,
                                               Set<String> labels,
                                               Auths auths) {
    return null;
  }

  @Override
  public CloseableIterable<Vertex> adjacencies(Iterable<Vertex> fromVertices,
                                               Node query, Direction direction,
                                               Set<String> labels,
                                               Auths auths) {
    return null;
  }

  @Override
  public void save(Iterable<Entity> entities) {
    super.save(entities);

    // here is where we want to store the edge index everytime an entity is saved

  }

  private boolean isEdge(Entity entity) {
    return entity.get(HEAD) != null &&
           entity.get(TAIL) != null &&
           entity.get(LABEL) != null;
  }
}
