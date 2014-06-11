/*
* Copyright (C) 2014 The Calrissian Authors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.calrissian.accumulorecipes.commons.support.metadata;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;
import static org.junit.Assert.assertEquals;

public class SimpleqMetadataSerDeTest {

    MetadataSerDe metadataSerDe = new SimpleMetadataSerDe(SIMPLE_TYPES);

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
