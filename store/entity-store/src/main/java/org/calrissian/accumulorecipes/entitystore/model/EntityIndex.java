package org.calrissian.accumulorecipes.entitystore.model;

import com.google.common.base.Preconditions;

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
