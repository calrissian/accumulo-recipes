package org.calrissian.accumulorecipes.graphstore.model;

import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;

public class EdgeEntity extends BaseEntity {

  public static final String HEAD = "head";
  public static final String TAIL = "tail";
  public static final String LABEL = "edgeLabel";

  public EdgeEntity(String type, String id, Entity head, Entity tail, String label) {
    super(type, id);

    put(new Tuple(HEAD, new EntityRelationship(head)));
    put(new Tuple(TAIL, new EntityRelationship(tail)));
    put(new Tuple(LABEL, label));
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
