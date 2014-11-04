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
package org.calrissian.accumulorecipes.commons.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.iterators.support.MetadataSerdeFactory;
import org.calrissian.accumulorecipes.commons.iterators.support.SimpleLexiMetadataSerdeFactory;
import org.calrissian.accumulorecipes.commons.support.tuple.Metadata;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

public class MetadataExpirationFilterTest {

    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException, InterruptedException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        connector.tableOperations().create("test");

        MetadataSerdeFactory metadataSerDe = new SimpleLexiMetadataSerdeFactory();

        IteratorSetting setting = new IteratorSetting(10, "filter", MetadataExpirationFilter.class);
        MetadataExpirationFilter.setMetadataSerdeFactory(setting, metadataSerDe.getClass());

        Map<String, Object> metadataMap = new HashMap<>();
        Metadata.Expiration.setExpiration(metadataMap, 1);

        BatchWriter writer = connector.createBatchWriter("test", 1000, 1000l, 10);
        Mutation m = new Mutation("a");
        m.put(new Text("b"), new Text(), currentTimeMillis() - 500, new Value(metadataSerDe.create().serialize(newArrayList(metadataMap))));

        Metadata.Expiration.setExpiration(metadataMap, 1000);

        m.put(new Text("b"), new Text(), currentTimeMillis() - 500, new Value(metadataSerDe.create().serialize(newArrayList(metadataMap))));

        m.put(new Text("c"), new Text(), currentTimeMillis() - 500, new Value("".getBytes()));

        writer.addMutation(m);
        writer.flush();

        Scanner scanner = connector.createScanner("test", new Authorizations());
        scanner.setRange(new Range("a"));

        assertEquals(2, Iterables.size(scanner));

        connector.tableOperations().attachIterator("test", setting);

        assertEquals(2, Iterables.size(scanner));

        Thread.sleep(1000);

        assertEquals(1, Iterables.size(scanner));

       assertEquals("c", Iterables.get(scanner, 0).getKey().getColumnFamily().toString());
    }




}
