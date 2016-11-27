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
package org.calrissian.accumulorecipes.thirdparty.pig.loader;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.commons.mock.MockRecordReader;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.hadoop.FeaturesInputFormat;
import org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore;
import org.calrissian.accumulorecipes.featurestore.model.Metric;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.calrissian.mango.domain.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class MetricsLoaderIT extends AccumuloInputFormat {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    MetricFeature metric;
    Job job;

    @Before
    public void setup() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        accumuloMiniClusterDriver.deleteAllTables();
        job = new Job();
    }

    @Test
    public void testGetNext() throws Exception {
        setUpJob();

        List<Pair<String, MetricFeature>> mocks = new ArrayList<Pair<String, MetricFeature>>();
        mocks.add(new Pair<String, MetricFeature>("", metric));

        MockRecordReader<String, MetricFeature> mockRecordReader = new MockRecordReader<String, MetricFeature>(mocks);


        MetricFeatureLoader loader = new MetricFeatureLoader();
        setLocation(loader, new Job(), "mockInst", "mockZk");
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        while((t = loader.getNext()) != null) {
            assertEquals(metric.getTimestamp(), t.get(0));
            assertEquals(TimeUnit.MINUTES.toString(), t.get(1));
            assertEquals(metric.getGroup(), t.get(2));
            assertEquals(metric.getType(), t.get(3));
            assertEquals(metric.getName(), t.get(4));
            assertEquals(metric.getVisibility(), t.get(5));
            assertEquals(metric.getVector().getSum(), t.get(6));
        }
    }

    @Test
    public void testSetLocation() throws URISyntaxException, IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {

        String zk = accumuloMiniClusterDriver.getZooKeepers();
        String inst = accumuloMiniClusterDriver.getInstanceName();

        Connector conn = accumuloMiniClusterDriver.getConnector();

        conn.tableOperations().create("features");
        Job job = new Job();

        MetricFeatureLoader loader = new MetricFeatureLoader();
        setLocation(loader, job, inst, zk);

        assertEquals(true, isConnectorInfoSet(job));
        assertEquals("root", getPrincipal(job));
        assertEquals(new PasswordToken(accumuloMiniClusterDriver.getRootPassword()), getAuthenticationToken(job));
        assertEquals(zk, getInstance(job).getZooKeepers());
        assertEquals(inst, getInstance(job).getInstanceName());
        assertEquals("features", getInputTableName(job));

    }

    private void setUpJob() throws Exception {
        Connector connector = accumuloMiniClusterDriver.getConnector();
        AccumuloFeatureStore store = new AccumuloFeatureStore(connector);
        store.initialize();
        metric = new MetricFeature(System.currentTimeMillis(), "group", "type", "name", "", new Metric(1));
        store.save(singleton(metric));
        store.flush();

        FeaturesInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
        FeaturesInputFormat.setZooKeeperInstance(job, accumuloMiniClusterDriver.getClientConfiguration());
        FeaturesInputFormat.setQueryInfo(job, new Date(System.currentTimeMillis() - 50000), new Date(), TimeUnit.MINUTES, "group", "type", "name", MetricFeature.class);

    }

    private void setLocation(LoadFunc loader, Job job, String inst, String zk) throws IOException, URISyntaxException {
        URI location = new URI("metrics://metrics?user=root&pass=&inst=" +
                inst + "&zk=" + zk  +
                "&timeUnit=MINUTES&group=group&start=2014-01-01&end=2014-01-02&auths=");
        loader.setLocation(location.toString(), job);
    }
}
