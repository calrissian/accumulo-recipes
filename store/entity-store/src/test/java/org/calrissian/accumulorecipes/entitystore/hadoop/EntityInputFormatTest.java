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
package org.calrissian.accumulorecipes.entitystore.hadoop;

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Before;
import org.junit.Test;

public class EntityInputFormatTest {

    public static Entity entity;
    public static Entity entity2;
    public static Entity entity3;

    @Before
    public void setup() {
        TestMapper.entities = new ArrayList<Entity>();
    }

    @Test
    public void testQuery() throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance("entityInst");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloEntityStore store = new AccumuloEntityStore(connector);
        entity = EntityBuilder.create("type", "id").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity));

        Job job = Job.getInstance();
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(EntityInputFormat.class);
        EntityInputFormat.setMockInstance(job, "entityInst");
        EntityInputFormat.setInputInfo(job, "root", "".getBytes(), new Authorizations());
        EntityInputFormat.setQueryInfo(job, Collections.singleton("type"),
                new QueryBuilder().eq("key1", "val1").build(), DEFAULT_SHARD_BUILDER, LEXI_TYPES);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertEquals(1, TestMapper.entities.size());
        assertEquals(TestMapper.entities.get(0).getId(), entity.getId());
        assertEquals(TestMapper.entities.get(0).getType(), entity.getType());
        assertEquals(new HashSet<Attribute>(TestMapper.entities.get(0).getAttributes()), new HashSet<Attribute>(entity.getAttributes()));

    }

    @Test
    public void testGetAllByType() throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {


        Instance instance = new MockInstance("entityInst1");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloEntityStore store = new AccumuloEntityStore(connector);
        entity = EntityBuilder.create("type", "id").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity));

        entity2 = EntityBuilder.create("type", "id2").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity2));

        entity3 = EntityBuilder.create("type1", "id").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity3));

        Job job = new Job(new Configuration());
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(EntityInputFormat.class);
        EntityInputFormat.setMockInstance(job, "entityInst1");
        EntityInputFormat.setInputInfo(job, "root", "".getBytes(), new Authorizations());
        EntityInputFormat.setQueryInfo(job, Collections.singleton("type"));
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertEquals(2, TestMapper.entities.size());
        System.out.println(TestMapper.entities);
        assertEquals(TestMapper.entities.get(0).getId(), entity.getId());
        assertEquals(TestMapper.entities.get(0).getType(), entity.getType());
        assertEquals(new HashSet<Attribute>(entity.getAttributes()), new HashSet<Attribute>(TestMapper.entities.get(1).getAttributes()));
        assertEquals(TestMapper.entities.get(1).getId(), entity2.getId());
        assertEquals(TestMapper.entities.get(1).getType(), entity2.getType());
        assertEquals(new HashSet<Attribute>(entity2.getAttributes()), new HashSet<Attribute>(TestMapper.entities.get(1).getAttributes()));
    }


    public static class TestMapper extends Mapper<Key, EntityWritable, Text, Text> {

        public static List<Entity> entities = new ArrayList<Entity>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Key key, EntityWritable value, Context context) throws IOException, InterruptedException {
            entities.add(value.get());
        }
    }
}
