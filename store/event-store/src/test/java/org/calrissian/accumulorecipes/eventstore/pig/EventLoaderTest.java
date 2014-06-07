package org.calrissian.accumulorecipes.eventstore.pig;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.accumulorecipes.test.MockRecordReader;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class EventLoaderTest {

    Event event;
    Configuration conf;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
    }

    @Test
    public void testGetNext() throws Exception {
        setUpJob();

        List<Pair<String, EventWritable>> mocks = new ArrayList<Pair<String, EventWritable>>();
        mocks.add(new Pair<String, EventWritable>("", new EventWritable(event)));

        MockRecordReader<String, EventWritable> mockRecordReader = new MockRecordReader<String, EventWritable>(mocks);

        EventLoader loader = new EventLoader();
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        int count = 0;

        Iterator<org.calrissian.mango.domain.Tuple> tuples = event.getTuples().iterator();
        while((t = loader.getNext()) != null) {
            org.calrissian.mango.domain.Tuple tuple = tuples.next();
            count++;
            if(count == 1) {
                assertEquals(event.getId(), t.get(0));
                assertEquals(event.getTimestamp(), t.get(1));
                assertEquals(tuple.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(tuple.getValue()), t.get(3));
                assertEquals(loader.registry.encode(tuple.getValue()), t.get(4));
            } else if(count == 2) {
                assertEquals(event.getId(), t.get(0));
                assertEquals(event.getTimestamp(), t.get(1));
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

        conn.tableOperations().create("eventStore_index");
        conn.tableOperations().create("eventStore_shard");
        Job job = new Job();
        URI location = new URI("event://eventStore_index/eventStore_shard?user=root&pass=&inst=" +
                inst + "&zk=" + zk  +
                "&query=q.eq('key','val')&start=2014-01-01&end=2014-01-02&auths=");
        EventLoader loader = new EventLoader();
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
        assertEquals("eventStore_shard", job.getConfiguration().get(CONFIG_PREFIX + "tablename"));

    }

    private void setUpJob() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {
        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloEventStore store = new AccumuloEventStore(connector);
        event = new BaseEvent(UUID.randomUUID().toString());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", false));
        store.save(singleton(event));

        EventInputFormat.setInputInfo(conf, "root", "".getBytes(), new Authorizations());
        EventInputFormat.setMockInstance(conf, "instName");
        EventInputFormat.setQueryInfo(conf, new Date(System.currentTimeMillis() - 50000), new Date(),
                new QueryBuilder().eq("key1", "val1").build());

    }
}
