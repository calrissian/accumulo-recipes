package org.calrissian.accumulorecipes.metricsstore.hadoop;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStore;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class MetricsInputFormatTest {

    public static final Metric metric = new Metric(System.currentTimeMillis(), "group", "type", "name", "", 1);

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloMetricStore store = new AccumuloMetricStore(connector);
        store.save(singleton(metric));

        Job job = new Job(new Configuration());
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(MetricsInputFormat.class);
        MetricsInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), new Authorizations());
        MetricsInputFormat.setQueryInfo(job.getConfiguration(), new Date(0), new Date(), MetricTimeUnit.MINUTES, "group", "type", "name");
        MetricsInputFormat.setMockInstance(job.getConfiguration(), "instName");
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertEquals(metric.getGroup(), TestMapper.metric.getGroup());
        assertEquals(metric.getType(), TestMapper.metric.getType());
        assertEquals(metric.getName(), TestMapper.metric.getName());
        assertEquals(metric.getVisibility(), TestMapper.metric.getVisibility());
        assertEquals(metric.getValue(), TestMapper.metric.getValue());

    }

    public static class TestMapper extends Mapper<Key, MetricWritable, Text, Text> {

        public static Metric metric;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Key key, MetricWritable value, Context context) throws IOException, InterruptedException {
            metric = value.get();
        }
    }
}
