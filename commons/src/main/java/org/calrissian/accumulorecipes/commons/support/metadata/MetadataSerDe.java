package org.calrissian.accumulorecipes.commons.support.metadata;

import org.calrissian.mango.types.TypeRegistry;

import java.io.Serializable;
import java.util.Map;


/**
 * A serializer/deserializer interface for a hashmap of metadata entries.
 */
public interface MetadataSerDe extends Serializable {

    void setTypeRegistry(TypeRegistry<String> registry);

    byte[] serialize(Map<String, Object> metadata);

    Map<String, Object> deserialize(byte[] bytes);
}
