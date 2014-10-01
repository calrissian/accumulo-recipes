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
package org.calrissian.accumulorecipes.featurestore.pig;

import com.google.common.collect.Multimap;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.hadoop.FeaturesInputFormat;
import org.calrissian.accumulorecipes.featurestore.model.Feature;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.mango.uri.support.UriUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collection;

import static org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.isConnectorInfoSet;

public class MetricFeatureLoader extends LoadFunc {

    public static final String USAGE = "Usage: metrics://tablePrefix?user=&pass=&inst=&zk=&timeUnit=&start=&end=&auths=&group=[&type=&name=]";

    protected RecordReader<Key, ? extends Feature> reader;
    protected TimeUnit timeUnit;

    @Override
    public void setLocation(String uri, Job job) throws IOException {

        String path = uri.substring(uri.indexOf("://") + 3, uri.indexOf("?"));

        if(!isConnectorInfoSet(AccumuloInputFormat.class, job.getConfiguration())){

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
                    timeUnit = TimeUnit.valueOf(timeUnitStr.toUpperCase());
                else
                    throw new IOException("A valid TimeUnit must be supplied. " + USAGE);

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

                FeaturesInputFormat.setZooKeeperInstance(job, accumuloInst, zookeepers);
                try {
                    FeaturesInputFormat.setInputInfo(job, accumuloUser, accumuloPass.getBytes(), new Authorizations(auths.getBytes()));
                } catch (AccumuloSecurityException e) {
                    throw new RuntimeException(e);
                }
                try {
                    FeaturesInputFormat.setQueryInfo(job, startDT.toDate(), endDT.toDate(), timeUnit, group, type, name, MetricFeature.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IOException("Location uri must begin with metrics://");
            }
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
        return new FeaturesInputFormat();
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
            MetricFeature metric = (MetricFeature) reader.getCurrentValue();
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
            t.append(metric.getVector().getSum());

            return t;
        } catch (InterruptedException e) {
            throw new IOException();
        }

    }
}
