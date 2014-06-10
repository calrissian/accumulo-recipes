package org.calrissian.accumulorecipes.commons.support.metadata;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;
import static org.junit.Assert.assertEquals;

public class BaseMetadataSerDeTest {

    MetadataSerDe metadataSerDe = new BaseMetadataSerDe(SIMPLE_TYPES);

    @Test
    public void testSimpleSerializationDeserialization() {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key1", "val1");
        map.put("key2", 5);
        map.put("key3", 10l);
        map.put("key4", 1.0);
        map.put("key5", true);

        byte[] bytes = metadataSerDe.serialize(map);

        Map<String,Object> actualMap = metadataSerDe.deserialize(bytes);
        assertEquals(map, actualMap);
    }
}
