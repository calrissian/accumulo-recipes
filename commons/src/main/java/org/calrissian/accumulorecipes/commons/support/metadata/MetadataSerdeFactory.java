package org.calrissian.accumulorecipes.commons.support.metadata;

import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;

/**
 * It's important that this class work with a default constructor as it will be getting newed up
 * through reflection.
 */
public interface MetadataSerdeFactory {

  MetadataSerDe create();
}
