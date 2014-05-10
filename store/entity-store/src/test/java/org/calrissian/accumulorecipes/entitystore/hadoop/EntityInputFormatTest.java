package org.calrissian.accumulorecipes.entitystore.hadoop;

import org.apache.accumulo.core.client.*;
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
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EntityInputFormatTest {

  public static Entity entity;

  @Test
  public void test() throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

    Instance instance = new MockInstance("instName");
    Connector connector = instance.getConnector("root", "".getBytes());
    AccumuloEntityStore store = new AccumuloEntityStore(connector);
    entity = new BaseEntity("type", "id");
    entity.put(new Tuple("key1", "val1", ""));
    entity.put(new Tuple("key2", false, ""));
    store.save(singleton(entity));

    Job job = new Job(new Configuration());
    job.setJarByClass(getClass());
    job.setMapperClass(TestMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(EntityInputFormat.class);
    EntityInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), new Authorizations());
    EntityInputFormat.setQueryInfo(job.getConfiguration(), Collections.singleton("type"),
            new QueryBuilder().eq("key1", "val1").build(), null);
    EntityInputFormat.setMockInstance(job.getConfiguration(), "instName");
    job.setOutputFormatClass(NullOutputFormat.class);

    job.submit();
    job.waitForCompletion(true);

    assertNotNull(TestMapper.entity);
    assertEquals(TestMapper.entity.getId(), entity.getId());
    assertEquals(TestMapper.entity.getType(), entity.getType());
    assertEquals(new HashSet<Tuple>(TestMapper.entity.getTuples()), new HashSet<Tuple>(entity.getTuples()));

  }

  public static class TestMapper extends Mapper<Key, EntityWritable, Text, Text> {

    public static Entity entity;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    @Override
    protected void map(Key key, EntityWritable value, Context context) throws IOException, InterruptedException {
      entity = value.get();
    }
  }
}
