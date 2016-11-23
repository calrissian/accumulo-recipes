/*
* Copyright (C) 2014 The Calrissian Authors
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

import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.entitystore.hadoop.EntityInputFormat;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.calrissian.accumulorecipes.test.MockRecordReader;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class EntityLoaderIT extends AccumuloInputFormat {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    Entity entity;
    Job job;



    @Before
    public void setup() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        accumuloMiniClusterDriver.deleteAllTables();
        job = Job.getInstance();
    }

    @Test
    public void testGetNext() throws Exception {
        setUpJob();

        List<Pair<String, EntityWritable>> mocks = new ArrayList<Pair<String, EntityWritable>>();
        mocks.add(new Pair<String, EntityWritable>("", new EntityWritable(entity)));

        MockRecordReader<String, EntityWritable> mockRecordReader = new MockRecordReader<String, EntityWritable>(mocks);

        EntityLoader loader = new EntityLoader("q.eq('key','val')");
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        int count = 0;

        Iterator<org.calrissian.mango.domain.Attribute> attributes = entity.getAttributes().iterator();
        while((t = loader.getNext()) != null) {
            org.calrissian.mango.domain.Attribute attribute = attributes.next();
            count++;
            if(count == 1) {
                assertEquals(entity.getType(), t.get(0));
                assertEquals(entity.getId(), t.get(1));
                assertEquals(attribute.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(attribute.getValue()), t.get(3));
                assertEquals(loader.registry.encode(attribute.getValue()), t.get(4));
            } else if(count == 2) {
                assertEquals(entity.getType(), t.get(0));
                assertEquals(entity.getId(), t.get(1));
                assertEquals(attribute.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(attribute.getValue()), t.get(3));
                assertEquals(loader.registry.encode(attribute.getValue()), t.get(4));
            }
        }

        assertEquals(2, count);
    }

    @Test
    public void testSetLocation() throws URISyntaxException, IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {

        String zk = accumuloMiniClusterDriver.getZooKeepers();
        String inst = accumuloMiniClusterDriver.getInstanceName();

        Connector conn = accumuloMiniClusterDriver.getConnector();

        conn.tableOperations().create(DEFAULT_IDX_TABLE_NAME);
        conn.tableOperations().create(DEFAULT_SHARD_TABLE_NAME);
        Job job = new Job();
        URI location = new URI("entity://eventStore_index/eventStore_shard?user=root&pass=&inst=" +
                inst + "&zk=" + zk  +
                "&types=myType&auths=");
        EntityLoader loader = new EntityLoader("q.eq('key','val')");
        loader.setLocation(location.toString(), job);
        loader.setLocation(location.toString(), job);   // make sure we don't fail if setLocation() called more than once


        assertEquals(true, isConnectorInfoSet(job));
        assertEquals("root", getPrincipal(job));
        assertEquals(new PasswordToken(accumuloMiniClusterDriver.getRootPassword()), getAuthenticationToken(job));
        assertEquals(inst, getInstance(job).getInstanceName());
        assertEquals(zk, getInstance(job).getZooKeepers());
        assertEquals(DEFAULT_SHARD_TABLE_NAME, getInputTableName(job));

    }

    private void setUpJob() throws Exception {
        Connector connector = accumuloMiniClusterDriver.getConnector();
        AccumuloEntityStore store = new AccumuloEntityStore(connector);
        entity = EntityBuilder.create("myType", UUID.randomUUID().toString())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", false))
            .build();
        store.save(singleton(entity));
        store.flush();

        EntityInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
        EntityInputFormat.setZooKeeperInstance(job, accumuloMiniClusterDriver.getClientConfiguration());
        EntityInputFormat.setQueryInfo(job, Collections.singleton("myType"),
                QueryBuilder.create().eq("key1", "val1").build(), DEFAULT_SHARD_BUILDER, LEXI_TYPES);

    }
}
