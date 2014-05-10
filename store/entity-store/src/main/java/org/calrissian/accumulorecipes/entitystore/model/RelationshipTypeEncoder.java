package org.calrissian.accumulorecipes.entitystore.model;

import org.calrissian.mango.types.TypeEncoder;
import org.calrissian.mango.types.exception.TypeDecodingException;
import org.calrissian.mango.types.exception.TypeEncodingException;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;

public class RelationshipTypeEncoder implements TypeEncoder<EntityRelationship, String>{

  @Override
  public String getAlias() {
    return "entityRelationship";
  }

  @Override
  public Class<EntityRelationship> resolves() {
    return EntityRelationship.class;
  }

  @Override
  public String encode(EntityRelationship entityRelationship) throws TypeEncodingException {
    return format("entity://%s#%s", entityRelationship.getTargetType(), entityRelationship.getTargetId());
  }

  @Override
  public EntityRelationship decode(String s) throws TypeDecodingException {
    String rel = s.substring(s.indexOf("entity://"), s.length());
    String[] parts = splitPreserveAllTokens(rel, "#");
    return new EntityRelationship(parts[0], parts[1]);
  }
}
