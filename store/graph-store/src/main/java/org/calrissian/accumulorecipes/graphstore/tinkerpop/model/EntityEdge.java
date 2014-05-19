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
package org.calrissian.accumulorecipes.graphstore.tinkerpop.model;


import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Entity;

import java.util.Iterator;

import static java.util.Collections.singletonList;

public class EntityEdge extends EntityElement implements Edge {

    public EntityEdge(Entity entity, GraphStore graphStore, Auths auths) {
        super(new EdgeEntity(entity), graphStore, auths);
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {

        EdgeEntity edgeEntity = (EdgeEntity) entity;
        EntityRelationship rel;
        if (direction == Direction.IN)
            rel = edgeEntity.getHead();
        else if (direction == Direction.OUT)
            rel = edgeEntity.getTail();
        else
            throw new RuntimeException("Direction.BOTH cannot be used when retrieving a vertex from an edge.");


        CloseableIterable<Entity> entities =
                graphStore.get(singletonList(new EntityIndex(rel.getTargetType(), rel.getTargetId())), null, auths);

        Iterator<Entity> entityItr = entities.iterator();

        Entity vertexEntity = null;
        if (entityItr.hasNext())
            vertexEntity = entityItr.next();

        entities.closeQuietly();

        if (vertexEntity != null)
            return new EntityVertex(vertexEntity, graphStore, auths);

        return null;
    }

    @Override
    public String getLabel() {
        return ((EdgeEntity) entity).getLabel();
    }
}
