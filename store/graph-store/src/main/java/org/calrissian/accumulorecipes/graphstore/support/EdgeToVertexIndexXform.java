/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.graphstore.support;

import com.google.common.base.Function;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIndex;
import org.calrissian.mango.domain.entity.EntityRelationship;

/**
 * This config function will pull the vertex index from the correct side of given the edge based on the given
 * direction and will structure the result as an index to be used to query the entity store.
 */
public class EdgeToVertexIndexXform implements Function<Entity, EntityIndex> {

  private org.calrissian.accumulorecipes.graphstore.model.Direction direction;

  public EdgeToVertexIndexXform(org.calrissian.accumulorecipes.graphstore.model.Direction direction) {
    this.direction = direction;
  }

  @Override
  public EntityIndex apply(Entity element) {

    String headOrTail;
    if (direction == Direction.IN)
      headOrTail = EdgeEntity.HEAD;
    else
      headOrTail = EdgeEntity.TAIL;

    EntityRelationship rel = element.<EntityRelationship>get(headOrTail).getValue();
    return new EntityIndex(rel.getType(), rel.getId());
  }
}
