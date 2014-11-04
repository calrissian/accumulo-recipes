package org.calrissian.accumulorecipes.commons.support.metadata;

import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class SimpleLexiMetadataSerdeFactory implements MetadataSerdeFactory {

  @Override
  public MetadataSerDe create() {
    return new SimpleMetadataSerDe(LEXI_TYPES);
  }
}
