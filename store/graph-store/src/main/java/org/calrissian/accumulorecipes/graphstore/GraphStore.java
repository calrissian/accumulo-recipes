package org.calrissian.accumulorecipes.graphstore;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;

import java.util.List;
import java.util.Set;

/**
 * A graph store allows adjacent edges and vertices to be fetched given a query
 */
public interface GraphStore extends EntityStore {

  CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIndex> fromVertices, Node query, Direction direction,
                                          Set<String> labels, Auths auths);

  CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIndex> fromVertices, Node query, Direction direction, Auths auths);

  CloseableIterable<Entity> adjacencies(List<EntityIndex> fromVertices, Node query, Direction direction,
                                        Set<String> labels, Auths auths);

  CloseableIterable<Entity> adjacencies(List<EntityIndex> fromVertices, Node query, Direction direction, Auths auths);
}
