package org.calrissian.accumulorecipes.commons.iterators.support;

import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.SimpleMetadataSerDe;

import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;

public class SimpleMetadataSerdeFactory implements MetadataSerdeFactory {

  @Override
  public MetadataSerDe create() {
    return new SimpleMetadataSerDe(SIMPLE_TYPES);
  }
}
