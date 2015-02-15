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
package org.calrissian.accumulorecipes.commons.hadoop;


import com.google.common.collect.ImmutableMap;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Test;

import java.io.IOException;

import static org.calrissian.accumulorecipes.commons.util.WritableUtils2.asWritable;
import static org.calrissian.accumulorecipes.commons.util.WritableUtils2.serialize;
import static org.junit.Assert.assertEquals;

public class EventWritableTest {

    @Test
    public void testSerializesAndDeserializes() throws IOException {

        Event event = new BaseEvent("id");
        event.put(new Tuple("key", "val", ImmutableMap.of("metaKey", "metaVal")));

        byte[] serialized = serialize(new EventWritable(event));

        Event actual = asWritable(serialized, EventWritable.class).get();
        assertEquals(event, actual);
    }


}
