package org.calrissian.accumulorecipes.graphstore;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.Edge;
import org.calrissian.accumulorecipes.graphstore.model.Vertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;

import java.util.Set;

public interface GraphStore {

  CloseableIterable<Edge> adjacentEdges(Iterable<Vertex> fromVertices, Node query, Direction direction,
                                        Set<String> labels, Auths auths);

  CloseableIterable<Vertex> adjacencies(Iterable<Vertex> fromVertices, Node query, Direction direction,
                                        Set<String> labels, Auths auths);
}
