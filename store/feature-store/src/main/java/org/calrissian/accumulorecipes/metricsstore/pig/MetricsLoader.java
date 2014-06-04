package org.calrissian.accumulorecipes.metricsstore.pig;

import com.google.common.collect.Multimap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.hadoop.FeatureWritable;
import org.calrissian.accumulorecipes.metricsstore.hadoop.MetricsInputFormat;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.uri.support.UriUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collection;

import static org.calrissian.mango.types.SimpleTypeEncoders.SIMPLE_TYPES;

public class MetricsLoader extends LoadFunc {

    public static final String USAGE = "Usage: metric://tablePrefix?user=&pass=&inst=&zk=&timeUnit=&start=&end=&auths=&group=[&type=&name=]";

    protected TypeRegistry<String> registry = SIMPLE_TYPES;
    protected RecordReader<Key, FeatureWritable> reader;
    protected MetricTimeUnit timeUnit;

    @Override
    public void setLocation(String uri, Job job) throws IOException {

        Configuration conf = job.getConfiguration();

        String path = uri.substring(uri.indexOf("://") + 3, uri.indexOf("?"));

        String[] indexAndShardTable = StringUtils.splitPreserveAllTokens(path, "/");
        if (indexAndShardTable.length != 1)
            throw new IOException("Path portion of URI must contain the metric table prefix " + USAGE);

        if (uri.startsWith("metrics")) {
            String queryPortion = uri.substring(uri.indexOf("?") + 1, uri.length());
            Multimap<String, String> queryParams = UriUtils.splitQuery(queryPortion);

            String accumuloUser = getProp(queryParams, "user");
            String accumuloPass = getProp(queryParams, "pass");
            String accumuloInst = getProp(queryParams, "inst");
            String zookeepers = getProp(queryParams, "zk");
            if (accumuloUser == null || accumuloPass == null || accumuloInst == null || zookeepers == null)
                throw new IOException("Some Accumulo connection information is missing. Must supply username, password, instance, and zookeepers. " + USAGE);

            String timeUnitStr = getProp(queryParams, "timeUnit");
            if(timeUnitStr != null)
                timeUnit = MetricTimeUnit.valueOf(timeUnitStr.toUpperCase());
            else
                throw new IOException("A valid MetricTimeUnit must be supplied. " + USAGE);

            String group = getProp(queryParams, "group");
            String type = getProp(queryParams, "type");
            String name = getProp(queryParams, "name");

            String startTime = getProp(queryParams, "start");
            String endTime = getProp(queryParams, "end");
            if (startTime == null || endTime == null)
                throw new IOException("Start and end times are required. " + USAGE);

            String auths = getProp(queryParams, "auths");
            if(auths == null)
                auths = ""; // default to empty auths

            DateTime startDT = DateTime.parse(startTime);
            DateTime endDT = DateTime.parse(endTime);

            MetricsInputFormat.setZooKeeperInstance(conf, accumuloInst, zookeepers);
            MetricsInputFormat.setInputInfo(conf, accumuloUser, accumuloPass.getBytes(), new Authorizations(auths.getBytes()));
            try {
                MetricsInputFormat.setQueryInfo(conf, startDT.toDate(), endDT.toDate(), timeUnit, group, type, name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IOException("Location uri must begin with metrics://");
        }
    }

    private String getProp(Multimap<String, String> queryParams, String propKey) {
        Collection<String> props = queryParams.get(propKey);
        if (props.size() > 0)
            return props.iterator().next();
        return null;
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new MetricsInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        reader = recordReader;
    }


    @Override
    public Tuple getNext() throws IOException {

        try {
            if (!reader.nextKeyValue())
                return null;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        try {
            Metric metric = reader.getCurrentValue().get();
            /**
             * Create the pig tuple and hydrate with metric details. The format of the tuple is as follows:
             * (timestamp, timeUnit, group, type, name, visibility, value)
             */
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(metric.getTimestamp());
            t.append(timeUnit.toString());
            t.append(metric.getGroup());
            t.append(metric.getType());
            t.append(metric.getName());
            t.append(metric.getVisibility());
            t.append(metric.getValue());

            return t;
        } catch (InterruptedException e) {
            throw new IOException();
        }

    }
}
