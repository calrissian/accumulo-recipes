package org.calrissian.accumulorecipes.metricsstore.pig;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.commons.mock.MockRecordReader;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.hadoop.FeatureWritable;
import org.calrissian.accumulorecipes.metricsstore.hadoop.MetricsInputFormat;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloFeatureStore;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.types.exception.TypeEncodingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class MetricsLoaderTest {

    Metric metric;
    Configuration conf;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
    }

    @Test
    public void testGetNext() throws AccumuloException, TableExistsException, TableNotFoundException, AccumuloSecurityException, IOException, InterruptedException, TypeEncodingException, URISyntaxException {
        setUpJob();

        List<Pair<String, FeatureWritable>> mocks = new ArrayList<Pair<String, FeatureWritable>>();
        mocks.add(new Pair<String, FeatureWritable>("", new FeatureWritable(metric)));

        MockRecordReader<String, FeatureWritable> mockRecordReader = new MockRecordReader<String, FeatureWritable>(mocks);


        MetricsLoader loader = new MetricsLoader();
        setLocation(loader, new Job(), "mockInst", "mockZk");
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        while((t = loader.getNext()) != null) {
            assertEquals(metric.getTimestamp(), t.get(0));
            assertEquals(MetricTimeUnit.MINUTES.toString(), t.get(1));
            assertEquals(metric.getGroup(), t.get(2));
            assertEquals(metric.getType(), t.get(3));
            assertEquals(metric.getName(), t.get(4));
            assertEquals(metric.getVisibility(), t.get(5));
            assertEquals(metric.getValue(), t.get(6));
        }
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

        conn.tableOperations().create("metrics");
        Job job = new Job();

        MetricsLoader loader = new MetricsLoader();
        setLocation(loader, job, inst, zk);

        cluster.stop();
        folder.delete();

        String CONFIG_PREFIX = AccumuloInputFormat.class.getSimpleName() + ".";
        assertEquals(true, job.getConfiguration().getBoolean(CONFIG_PREFIX + "instanceConfigured", false));
        assertEquals(true, job.getConfiguration().getBoolean(CONFIG_PREFIX + "configured", false));
        assertEquals("root", job.getConfiguration().get(CONFIG_PREFIX + "username"));
        assertEquals("", job.getConfiguration().get(CONFIG_PREFIX + "password"));
        assertEquals(zk, job.getConfiguration().get(CONFIG_PREFIX + "zooKeepers"));
        assertEquals(inst, job.getConfiguration().get(CONFIG_PREFIX + "instanceName"));
        assertEquals("metrics", job.getConfiguration().get(CONFIG_PREFIX + "tablename"));

    }

    private void setUpJob() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {
        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloFeatureStore store = new AccumuloFeatureStore(connector);
        metric = new Metric(System.currentTimeMillis(), "group", "type", "name", "", 1);
        store.save(singleton(metric));

        MetricsInputFormat.setInputInfo(conf, "root", "".getBytes(), new Authorizations());
        MetricsInputFormat.setMockInstance(conf, "instName");
        MetricsInputFormat.setQueryInfo(conf, new Date(System.currentTimeMillis() - 50000), new Date(), MetricTimeUnit.MINUTES, "group", "type", "name");

    }

    private void setLocation(LoadFunc loader, Job job, String inst, String zk) throws IOException, URISyntaxException {
        URI location = new URI("metrics://metrics?user=root&pass=&inst=" +
                inst + "&zk=" + zk  +
                "&timeUnit=MINUTES&group=group&start=2014-01-01&end=2014-01-02&auths=");
        loader.setLocation(location.toString(), job);
    }
}
