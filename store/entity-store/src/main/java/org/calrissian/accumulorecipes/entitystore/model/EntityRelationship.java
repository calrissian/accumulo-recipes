package org.calrissian.accumulorecipes.entitystore.model;

import org.calrissian.mango.domain.Entity;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Models a relationship to an entity.
 */
public class EntityRelationship {

  private String targetType;
  private String targetId;

  public EntityRelationship(String targetType, String targetId) {
    checkNotNull(targetType);
    checkNotNull(targetId);
    this.targetType = targetType;
    this.targetId = targetId;
  }

  public EntityRelationship(Entity entity) {
    this(entity.getType(),entity.getId());
  }

  public String getTargetType() {
    return targetType;
  }

  public String getTargetId() {
    return targetId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EntityRelationship that = (EntityRelationship) o;

    if (targetId != null ? !targetId.equals(that.targetId) : that.targetId != null) return false;
    if (targetType != null ? !targetType.equals(that.targetType) : that.targetType != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = targetType != null ? targetType.hashCode() : 0;
    result = 31 * result + (targetId != null ? targetId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "EntityRelationship{" +
            "targetType='" + targetType + '\'' +
            ", targetId='" + targetId + '\'' +
            '}';
  }
}
