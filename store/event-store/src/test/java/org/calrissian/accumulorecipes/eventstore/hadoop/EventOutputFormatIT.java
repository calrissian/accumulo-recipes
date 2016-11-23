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
package org.calrissian.accumulorecipes.eventstore.hadoop;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.commons.support.attribute.MetadataBuilder;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.calrissian.mango.types.TypeRegistry;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

public class EventOutputFormatIT {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    public static final String A = "A";
    private static Map<String, String> META = new MetadataBuilder().setVisibility(A).build();
    private static Auths DEFAULT_AUTHS = new Auths(A);

    public static final String INST_NAME = "instName";
    public static final String PRINCIPAL = "root";
    public static final String VAL_1 = "VaL1";
    public static final String KEY_1 = "key1";
    public static final String TYPE = "type";
    Connector connector;

    public static final TypeRegistry<String> TYPE_REGISTRY = new TypeRegistry<>(LexiTypeEncoders.stringEncoder());


    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        accumuloMiniClusterDriver.deleteAllTables();
        connector = accumuloMiniClusterDriver.getConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(A));
    }

    @Test
    public void mapReduceFromTextFile() throws IOException, AccumuloSecurityException, ClassNotFoundException, InterruptedException, TableExistsException, AccumuloException, TableNotFoundException {
        EventStore eventStore = new AccumuloEventStore(connector);
        Job job = Job.getInstance(new Configuration());
        runJob(job,eventStore);
    }

    @Test
    public void mapReduceFromTextFileWithEventOutputFormatParams() throws IOException, AccumuloSecurityException, ClassNotFoundException, InterruptedException, TableExistsException, AccumuloException, TableNotFoundException {
        StoreConfig storeConfig = new StoreConfig(1,30L,30L,1);
        String tableIdx = "tableIdx";
        String shardTable = "table";
        EventStore eventStore = new AccumuloEventStore.Builder(connector)
                .setIndexTable(tableIdx)
                .setShardTable(shardTable)
                .setStoreConfig(storeConfig)
                .setTypeRegistry(TYPE_REGISTRY)
                .build();
        Job job = Job.getInstance(new Configuration());
        EventOutputFormat.setTables(job, tableIdx, shardTable);
        EventOutputFormat.setStoreConfig(job,storeConfig);
        EventOutputFormat.setTypeRegistry(job,TYPE_REGISTRY);
        runJob(job,eventStore);
    }

    public void runJob(Job job, EventStore eventStore) throws IOException, AccumuloSecurityException, ClassNotFoundException, InterruptedException, TableExistsException, AccumuloException, TableNotFoundException {
        File dir = temporaryFolder.newFolder("input");

        FileOutputStream fileOutputStream = new FileOutputStream(new File(dir,"uuids.txt"));
        PrintWriter printWriter = new PrintWriter(fileOutputStream);
        int countTotalResults = 100;
        try {
            for (int i = 0; i < countTotalResults; i++) {
                printWriter.println(""+i);
            }
        } finally {
            printWriter.flush();
            fileOutputStream.close();
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        fs.setWorkingDirectory(new Path(dir.getAbsolutePath()));

        Path inputPath = fs.makeQualified(new Path(dir.getAbsolutePath()));  // local path


        EventOutputFormat.setZooKeeperInstance(job, accumuloMiniClusterDriver.getClientConfiguration());
        EventOutputFormat.setConnectorInfo(job, PRINCIPAL, new PasswordToken(accumuloMiniClusterDriver.getRootPassword()));
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(EventWritable.class);
        job.setOutputFormatClass(EventOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);

        job.submit();
        job.waitForCompletion(true);

        Iterable<Event> itr = eventStore.query(new Date(currentTimeMillis() - 25000),
                new Date(), Collections.singleton(TYPE), QueryBuilder.create().and().eq(KEY_1, VAL_1).end().build(), null, DEFAULT_AUTHS);

        List<Event> queryResults = Lists.newArrayList(itr);
        assertEquals(countTotalResults,queryResults.size());
    }

    public static class TestMapper extends Mapper<LongWritable, Text, Text, EventWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Event event = EventBuilder.create(TYPE, key.toString(), System.currentTimeMillis())
                    .attr(new Attribute(KEY_1, VAL_1, META))
                    .attr(new Attribute("key2", "value2",META)).build();

            context.write(value,new EventWritable(event));
        }
    }



}
