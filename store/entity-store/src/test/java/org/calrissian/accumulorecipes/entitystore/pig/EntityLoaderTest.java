package org.calrissian.accumulorecipes.entitystore.pig;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.entitystore.hadoop.EntityInputFormat;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.test.MockRecordReader;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;

public class EntityLoaderTest {

    Entity entity;
    Configuration conf;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
    }

    @Test
    public void testGetNext() throws Exception {
        setUpJob();

        List<Pair<String, EntityWritable>> mocks = new ArrayList<Pair<String, EntityWritable>>();
        mocks.add(new Pair<String, EntityWritable>("", new EntityWritable(entity)));

        MockRecordReader<String, EntityWritable> mockRecordReader = new MockRecordReader<String, EntityWritable>(mocks);

        EntityLoader loader = new EntityLoader();
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        int count = 0;

        Iterator<org.calrissian.mango.domain.Tuple> tuples = entity.getTuples().iterator();
        while((t = loader.getNext()) != null) {
            org.calrissian.mango.domain.Tuple tuple = tuples.next();
            count++;
            if(count == 1) {
                assertEquals(entity.getType(), t.get(0));
                assertEquals(entity.getId(), t.get(1));
                assertEquals(tuple.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(tuple.getValue()), t.get(3));
                assertEquals(loader.registry.encode(tuple.getValue()), t.get(4));
            } else if(count == 2) {
                assertEquals(entity.getType(), t.get(0));
                assertEquals(entity.getId(), t.get(1));
                assertEquals(tuple.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(tuple.getValue()), t.get(3));
                assertEquals(loader.registry.encode(tuple.getValue()), t.get(4));
            }
        }

        assertEquals(2, count);
    }

    @Test
    public void testSetLocation() throws URISyntaxException, IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {

        TemporaryFolder folder = new TemporaryFolder();
        folder.create();

        MiniAccumuloCluster cluster = new MiniAccumuloCluster(folder.getRoot(), "");
        cluster.start();

        String zk = cluster.getZooKeepers();
        String inst = cluster.getInstanceName();

        Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        Connector conn = instance.getConnector("root", "".getBytes());

        conn.tableOperations().create(DEFAULT_IDX_TABLE_NAME);
        conn.tableOperations().create(DEFAULT_SHARD_TABLE_NAME);
        Job job = new Job();
        URI location = new URI("entity://eventStore_index/eventStore_shard?user=root&pass=&inst=" +
                inst + "&zk=" + zk  +
                "&query=q.eq('key','val')&types=myType&auths=");
        EntityLoader loader = new EntityLoader();
        loader.setLocation(location.toString(), job);


        cluster.stop();
        folder.delete();

        String CONFIG_PREFIX = AccumuloInputFormat.class.getSimpleName() + ".";
        assertEquals(true, job.getConfiguration().getBoolean(CONFIG_PREFIX + "instanceConfigured", false));
        assertEquals(true, job.getConfiguration().getBoolean(CONFIG_PREFIX + "configured", false));
        assertEquals("root", job.getConfiguration().get(CONFIG_PREFIX + "username"));
        assertEquals("", job.getConfiguration().get(CONFIG_PREFIX + "password"));
        assertEquals(zk, job.getConfiguration().get(CONFIG_PREFIX + "zooKeepers"));
        assertEquals(inst, job.getConfiguration().get(CONFIG_PREFIX + "instanceName"));
        assertEquals(DEFAULT_SHARD_TABLE_NAME, job.getConfiguration().get(CONFIG_PREFIX + "tablename"));

    }

    private void setUpJob() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {
        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloEntityStore store = new AccumuloEntityStore(connector);
        entity = new BaseEntity("myType");
        entity.put(new Tuple("key1", "val1"));
        entity.put(new Tuple("key2", false));
        store.save(singleton(entity));

        EntityInputFormat.setInputInfo(conf, "root", "".getBytes(), new Authorizations());
        EntityInputFormat.setMockInstance(conf, "instName");
        EntityInputFormat.setQueryInfo(conf, Collections.singleton("myType"),
                new QueryBuilder().eq("key1", "val1").build(), null, LEXI_TYPES);

    }
}
