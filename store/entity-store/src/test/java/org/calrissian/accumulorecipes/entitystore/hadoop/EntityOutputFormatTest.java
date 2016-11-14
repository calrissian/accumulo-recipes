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
package org.calrissian.accumulorecipes.entitystore.hadoop;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.calrissian.accumulorecipes.commons.support.attribute.MetadataBuilder;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EntityOutputFormatTest {

    public static final String INST_NAME = "instName";
    public static final String PRINCIPAL = "root";
    public static final String PASSWORD = "";
    public static final String VAL_1 = "VaL1";
    public static final String KEY_1 = "key1";
    public static final String TYPE = "type";

    private static Map<String, String> meta = new MetadataBuilder().setVisibility("A").build();
    private static Auths DEFAULT_AUTHS = new Auths("A");

    MockInstance mockInstance;
    Connector connector;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws AccumuloSecurityException, AccumuloException {
        mockInstance =new MockInstance(INST_NAME);
        connector = mockInstance.getConnector(PRINCIPAL, new PasswordToken(PASSWORD));
    }

    @Test
    public void mapReduceFromTextFile() throws IOException, AccumuloSecurityException, ClassNotFoundException, InterruptedException, TableExistsException, AccumuloException, TableNotFoundException {
        EntityStore eventStore = new AccumuloEntityStore(connector);
        Job job = Job.getInstance(new Configuration());
        runJob(job,eventStore);
    }

    @Test
    public void mapReduceFromTextFileWithEventOutputFormatParams() throws IOException, AccumuloSecurityException, ClassNotFoundException, InterruptedException, TableExistsException, AccumuloException, TableNotFoundException {
        StoreConfig storeConfig = new StoreConfig(1,30L,30L,1);
        String tableIdx = "tableIdx";
        String shardTable = "table";
        EntityStore eventStore = new AccumuloEntityStore.Builder(connector)
                .setIndexTable(tableIdx)
                .setShardTable(shardTable)
                .setStoreConfig(storeConfig)
                .build();
        Job job = Job.getInstance(new Configuration());
        EntityOutputFomat.setTables(job, tableIdx, shardTable);
        EntityOutputFomat.setStoreConfig(job,storeConfig);
        runJob(job,eventStore);
    }


    public void runJob(Job job, EntityStore entityStore) throws IOException, AccumuloSecurityException, ClassNotFoundException, InterruptedException, TableExistsException, AccumuloException, TableNotFoundException {
        File dir = temporaryFolder.newFolder("input");

        FileOutputStream fileOutputStream = new FileOutputStream(new File(dir,"uuids.txt"));
        PrintWriter printWriter = new PrintWriter(fileOutputStream);
        int countTotalResults = 1000;
        try {
            for (int i = 0; i < countTotalResults; i++) {
                printWriter.println(i+"");
            }
        } finally {
            printWriter.flush();
            fileOutputStream.close();
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        fs.setWorkingDirectory(new Path(dir.getAbsolutePath()));

        Path inputPath = fs.makeQualified(new Path(dir.getAbsolutePath()));  // local path


        EntityOutputFomat.setMockInstance(job, INST_NAME);
        EntityOutputFomat.setConnectorInfo(job, PRINCIPAL, new PasswordToken(PASSWORD));
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(EntityWritable.class);
        job.setOutputFormatClass(EntityOutputFomat.class);

        FileInputFormat.setInputPaths(job, inputPath);

        job.submit();
        job.waitForCompletion(true);

        Node query = QueryBuilder.create().and().eq(KEY_1, VAL_1).end().build();

        Iterable<Entity> itr = entityStore.query(Collections.singleton(TYPE), query, null, new Auths("A"));

        List<Entity> queryResults = Lists.newArrayList(itr);
        assertEquals(countTotalResults,queryResults.size());
    }


    public static class TestMapper extends Mapper<LongWritable, Text, Text, EntityWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Entity entity = EntityBuilder.create(TYPE, value.toString())
                    .attr(new Attribute(KEY_1, VAL_1, meta))
                    .attr(new Attribute("key2", "val2", meta))
                    .build();

            context.write(value,new EntityWritable(entity));
        }
    }
}
