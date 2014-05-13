package org.calrissian.accumulorecipes.entitystore.support;

import com.google.common.base.Function;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.mango.domain.Entity;

public class TransformUtils {

  private TransformUtils() {}

  public static Function<Entity, EntityIndex> entityToEntityIndex = new Function<Entity, EntityIndex>() {
    @Override
    public EntityIndex apply(Entity entity) {
      return new EntityIndex(entity);
    }
  };
}
