/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /**
     * Returns the adjacent edges where the edges match some given query. Edges returned will match the given labels
     */
    CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIndex> fromVertices, Node query, Direction direction,
                                                Set<String> labels, Auths auths);

    /**
     * Returns edges adjacent to the given vertices that match the given query in the given direction.
     */
    CloseableIterable<EdgeEntity> adjacentEdges(List<EntityIndex> fromVertices, Node query, Direction direction, Auths auths);

    /**
     * Returns vertices adjacent to the given vertices where the edges connecting the vertices match the given query
     * with the given direction. Adjacent vertices will have an edge connection to the input vertices which match
     * the labels given.
     */
    CloseableIterable<Entity> adjacencies(List<EntityIndex> fromVertices, Node query, Direction direction,
                                          Set<String> labels, Auths auths);

    /**
     * Returns vertices adjacent to the given verties where the edges connecting the vertices match the given query
     * with the given direction.
     */
    CloseableIterable<Entity> adjacencies(List<EntityIndex> fromVertices, Node query, Direction direction, Auths auths);
}
