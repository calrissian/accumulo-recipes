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
package org.calrissian.accumulorecipes.thirdparty.tinkerpop.model;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.thirdparty.tinkerpop.query.EntityVertexQuery;
import org.calrissian.mango.domain.entity.Entity;


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
