/*
 * Copyright (C) 2013 The Calrissian Authors
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
package org.calrissian.accumulorecipes.lastn.impl;


import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.domain.BaseEvent;
import org.calrissian.mango.domain.Event;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AccumuloLastNStoreTest {

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Test
    public void test() throws Exception {
        AccumuloLastNStore lastNStore = new AccumuloLastNStore(getConnector(), 3);

        Event entry1 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
        entry1.put(new Tuple("key1", "val1", ""));
        entry1.put(new Tuple("key3", "val3", ""));

        Event entry2 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000);
        entry2.put(new Tuple("key1", "val1", ""));
        entry2.put(new Tuple("key3", "val3", ""));

        Event entry3 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000);
        entry3.put(new Tuple("key1", "val1", ""));
        entry3.put(new Tuple("key3", "val3", ""));

        Event entry4 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000);
        entry4.put(new Tuple("key1", "val1", ""));
        entry4.put(new Tuple("key3", "val3", ""));

        lastNStore.put("index1", entry1);
        lastNStore.put("index1", entry2);
        lastNStore.put("index1", entry3);
        lastNStore.put("index1", entry4);

        List<Event> results = Lists.newArrayList(lastNStore.get("index1", new Auths()));
        assertEquals(3, results.size());
        assertEquals(entry4, results.get(0));
        assertEquals(entry3, results.get(1));
        assertEquals(entry2, results.get(2));
    }
}
