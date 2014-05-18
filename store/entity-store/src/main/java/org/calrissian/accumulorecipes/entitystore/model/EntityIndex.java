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
package org.calrissian.accumulorecipes.entitystore.model;

import org.calrissian.mango.domain.Entity;

import static com.google.common.base.Preconditions.checkNotNull;

public class EntityIndex {

  private String type;
  private String id;

  public EntityIndex(String type, String id) {
    checkNotNull(type);
    checkNotNull(id);
    this.type = type;
    this.id = id;
  }

  public EntityIndex(Entity entity) {
    this(entity.getType(), entity.getId());
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EntityIndex that = (EntityIndex) o;

    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    if (type != null ? !type.equals(that.type) : that.type != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (id != null ? id.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "EntityIndex{" +
            "type='" + type + '\'' +
            ", id='" + id + '\'' +
            '}';
  }
}
