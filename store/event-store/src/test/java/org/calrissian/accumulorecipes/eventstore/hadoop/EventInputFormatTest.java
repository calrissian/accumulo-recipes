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
package org.calrissian.accumulorecipes.eventstore.hadoop;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import static java.util.Collections.singleton;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EventInputFormatTest {

    public static Event event;

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloEventStore store = new AccumuloEventStore(connector);
        event = new BaseEvent(UUID.randomUUID().toString());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", false, ""));
        store.save(singleton(event));

        Job job = new Job(new Configuration());
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(EventInputFormat.class);
        EventInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), new Authorizations());
        EventInputFormat.setMockInstance(job.getConfiguration(), "instName");
        EventInputFormat.setQueryInfo(job.getConfiguration(), new Date(System.currentTimeMillis() - 50000), new Date(),
                new QueryBuilder().eq("key1", "val1").build(), null, LEXI_TYPES);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertNotNull(TestMapper.entry);
        assertEquals(TestMapper.entry.getId(), event.getId());
        assertEquals(TestMapper.entry.getTimestamp(), event.getTimestamp());
        assertEquals(new HashSet<Tuple>(TestMapper.entry.getTuples()), new HashSet<Tuple>(event.getTuples()));

    }

    public static class TestMapper extends Mapper<Key, EventWritable, Text, Text> {

        public static Event entry;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Key key, EventWritable value, Context context) throws IOException, InterruptedException {
            entry = value.get();
        }
    }
}
