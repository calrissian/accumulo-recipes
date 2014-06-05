package org.calrissian.accumulorecipes.featurestore.hadoop;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.Metric;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class MetricsInputFormatTest {

    public static final MetricFeature metric = new MetricFeature(currentTimeMillis(), "group", "type", "name", "", new Metric(1));

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloFeatureStore store = new AccumuloFeatureStore(connector);
        store.initialize();
        store.save(singleton(metric));

        Job job = new Job(new Configuration());
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(FeaturesInputFormat.class);
        FeaturesInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), new Authorizations());
        FeaturesInputFormat.setQueryInfo(job.getConfiguration(), new Date(0), new Date(), TimeUnit.MINUTES, "group", "type", "name", MetricFeature.class);
        FeaturesInputFormat.setMockInstance(job.getConfiguration(), "instName");
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertEquals(metric.getGroup(), TestMapper.metric.getGroup());
        assertEquals(metric.getType(), TestMapper.metric.getType());
        assertEquals(metric.getName(), TestMapper.metric.getName());
        assertEquals(metric.getVisibility(), TestMapper.metric.getVisibility());
        assertEquals(metric.getVector(), TestMapper.metric.getVector());

    }

    public static class TestMapper extends Mapper<Key, MetricFeature, Text, Text> {

        public static MetricFeature metric;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Key key, MetricFeature value, Context context) throws IOException, InterruptedException {
            metric = value;
        }
    }
}
