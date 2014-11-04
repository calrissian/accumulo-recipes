package org.calrissian.accumulorecipes.commons.support.metadata;

import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;

public class SimpleMetadataSerdeFactory implements MetadataSerdeFactory {

  @Override
  public MetadataSerDe create() {
    return new SimpleMetadataSerDe(SIMPLE_TYPES);
  }
}
