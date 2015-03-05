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
package org.calrissian.accumulorecipes.commons.support.attribute.metadata;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SimpleMetadataSerDeTest {

    MetadataSerdeFactory metadataSerDe = new SimpleMetadataSerdeFactory();

    @Test
    public void testSimpleSerializationDeserialization() {

        Map<String, String> map = new HashMap<String, String>();
        map.put("key1", "val1");
        map.put("key2", "5");
        map.put("key3", "10");
        map.put("key4", "1.0");
        map.put("key5", "true");

        byte[] bytes = metadataSerDe.create().serialize(map);

        Map<String,String> actualMap = metadataSerDe.create().deserialize(bytes);
        assertEquals(map, actualMap);
    }
}
