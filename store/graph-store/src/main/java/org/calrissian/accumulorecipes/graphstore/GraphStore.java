package org.calrissian.accumulorecipes.graphstore;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;

import java.util.Set;

public interface GraphStore {

  CloseableIterable<Entity> adjacentEdges(Iterable<Entity> fromVertices, Node query, Direction direction,
                                          Set<String> labels, Auths auths);

  CloseableIterable<EdgeEntity> adjacentEdges(Iterable<Entity> fromVertices, Node query, Direction direction, Auths auths);

  CloseableIterable<Entity> adjacencies(Iterable<Entity> fromVertices, Node query, Direction direction,
                                        Set<String> labels, Auths auths);

  CloseableIterable<Entity> adjacencies(Iterable<Entity> fromVertices, Node query, Direction direction, Auths auths);
}
