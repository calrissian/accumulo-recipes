package org.calrissian.accumulorecipes.entitystore.support;

import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransformUtilsTest {

  @Test
  public void test() {

    Entity entity = new BaseEntity("type", "id");

    EntityIndex index = TransformUtils.entityToEntityIndex.apply(entity);
    assertEquals(entity.getType(), index.getType());
    assertEquals(entity.getId(), index.getId());
  }
}
