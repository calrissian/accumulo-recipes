/*
 * Copyright (C) 2016 The Calrissian Authors
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

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;

public class EntityInputFormatIT {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    public static Entity entity;
    public static Entity entity2;
    public static Entity entity3;

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        accumuloMiniClusterDriver.deleteAllTables();
        TestMapper.entities = new ArrayList<Entity>();
    }

    @Test
    public void testQuery() throws Exception {

        Connector connector = accumuloMiniClusterDriver.getConnector();
        AccumuloEntityStore store = new AccumuloEntityStore(connector);
        entity = EntityBuilder.create("type", "id").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity));
        store.flush();

        Job job = Job.getInstance();
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(EntityInputFormat.class);
        EntityInputFormat.setZooKeeperInstance(job,accumuloMiniClusterDriver.getClientConfiguration());
        EntityInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
        EntityInputFormat.setQueryInfo(job, Collections.singleton("type"),
                QueryBuilder.create().eq("key1", "val1").build(), DEFAULT_SHARD_BUILDER, LEXI_TYPES);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertEquals(1, TestMapper.entities.size());
        assertEquals(TestMapper.entities.get(0).getId(), entity.getId());
        assertEquals(TestMapper.entities.get(0).getType(), entity.getType());
        assertEquals(new HashSet<Attribute>(TestMapper.entities.get(0).getAttributes()), new HashSet<Attribute>(entity.getAttributes()));

    }

    @Test
    public void testGetAllByType() throws Exception {


        Connector connector = accumuloMiniClusterDriver.getConnector();
        AccumuloEntityStore store = new AccumuloEntityStore(connector);
        entity = EntityBuilder.create("type", "id").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity));

        entity2 = EntityBuilder.create("type", "id2").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity2));

        entity3 = EntityBuilder.create("type1", "id").attr(new Attribute("key1", "val1")).attr(new Attribute("key2", false)).build();
        store.save(singleton(entity3));

        store.flush();

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(EntityInputFormat.class);
        EntityInputFormat.setZooKeeperInstance(job,accumuloMiniClusterDriver.getClientConfiguration());
        EntityInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
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
