package org.calrissian.accumulorecipes.graphstore.model;

import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;

import static com.google.common.base.Preconditions.checkNotNull;

public class EdgeEntity extends BaseEntity {

  public static final String HEAD = "head";
  public static final String TAIL = "tail";
  public static final String LABEL = "edgeLabel";

  public EdgeEntity(String type, String id, Entity head, String headVis, Entity tail, String tailVis, String label) {
    super(type, id);

    checkNotNull(head);
    checkNotNull(headVis);
    checkNotNull(tail);
    checkNotNull(tailVis);
    checkNotNull(label);

    put(new Tuple(HEAD, new EntityRelationship(head), headVis));
    put(new Tuple(TAIL, new EntityRelationship(tail), tailVis));
    put(new Tuple(LABEL, label));
  }

  public EdgeEntity(String type, String id, Entity head, Entity tail, String label) {
    this(type, id, head, "", tail, "", label);
  }

  public EdgeEntity(Entity entity) {
    super(entity.getType(), entity.getId());
    putAll(entity.getTuples());
  }

  public EntityRelationship getHead() {
    if(this.get(HEAD) != null)
      return this.<EntityRelationship>get(HEAD).getValue();
    return null;
  }

  public EntityRelationship getTail() {
    if(this.get(TAIL) != null)
      return this.<EntityRelationship>get(TAIL).getValue();
    return null;
  }

  public String getLabel() {
    if(this.get(LABEL) != null)
      return this.<String>get(LABEL).getValue();
    return null;
  }
}
