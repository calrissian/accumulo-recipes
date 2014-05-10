package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.tinkerpop.blueprints.Element;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class EntityElement implements Element {

  protected Entity entity;
  protected GraphStore graphStore;

  public EntityElement(Entity entity, GraphStore graphStore) {
    checkNotNull(entity);
    checkNotNull(graphStore);
    this.entity = entity;
    this.graphStore = graphStore;
  }

  @Override
  public <T> T getProperty(String s) {
    return entity.<T>get(s).getValue();
  }

  @Override
  public Set<String> getPropertyKeys() {
    return entity.keys();
  }

  @Override
  public void setProperty(String s, Object o) {
    entity.put(new Tuple(s, o));
  }

  @Override
  public <T> T removeProperty(String s) {
    return (T)entity.remove(s).getValue();
  }

  @Override
  public void remove() {

    //TODO: Figure out what this method does
  }

  @Override
  public Object getId() {
    return new EntityIndex(entity);
  }
}
