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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerdeFactory;
import org.calrissian.accumulorecipes.commons.support.metadata.SimpleMetadataSerdeFactory;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.accumulo.core.client.admin.TimeType.LOGICAL;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Expiration;
import static org.junit.Assert.assertEquals;

public class MetadataExpirationFilterTest {

    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException, InterruptedException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        connector.tableOperations().create("test", false, LOGICAL);

        MetadataSerdeFactory metadataSerDe = new SimpleMetadataSerdeFactory();

        IteratorSetting setting = new IteratorSetting(10, "filter", MetadataExpirationFilter.class);
        MetadataExpirationFilter.setMetadataSerdeFactory(setting, metadataSerDe.getClass());

        IteratorSetting setting2 = new IteratorSetting(9, "combiner", MetadataCombiner.class);
        MetadataCombiner.setMetadataSerdeFactory(setting2, metadataSerDe.getClass());
        MetadataCombiner.setCombineAllColumns(setting2, true);

        connector.tableOperations().attachIterator("test", setting2);

        Map<String, Object> metadataMap = new HashMap<String,Object>();
        Expiration.setExpiration(metadataMap, 1);

        BatchWriter writer = connector.createBatchWriter("test", 1000, 1000l, 10);
        Mutation m = new Mutation("a");
        m.put(new Text("b"), new Text(), new Value(metadataSerDe.create().serialize(newArrayList(metadataMap))));

        Expiration.setExpiration(metadataMap, 1500);

        m.put(new Text("c"), new Text(), new Value("".getBytes()));

        writer.addMutation(m);

        Mutation m2 = new Mutation("a");
        m2.put(new Text("b"), new Text(), new Value(metadataSerDe.create().serialize(newArrayList(metadataMap))));
        writer.addMutation(m2);

        writer.flush();

        Scanner scanner = connector.createScanner("test", new Authorizations());
        scanner.setRange(new Range("a"));

        System.out.println("YAY");
        for(Map.Entry<Key,Value> entry : scanner) {
          System.out.println(entry);
        }

        assertEquals(2, Iterables.size(scanner));


        Map.Entry<Key,Value> entry = scanner.iterator().next();

        connector.tableOperations().attachIterator("test", setting);

        int count = 0;
        System.out.println("YAY");
        for(Map.Entry<Key,Value> theENtry : scanner) {
          count++;
          System.out.println(theENtry);
        }

        assertEquals(2, metadataSerDe.create().deserialize(entry.getValue().get()).size());
        assertEquals(2, Iterables.size(scanner));

        Thread.sleep(1500);

      System.out.println(count);
      System.out.println("RESULTS");
      count = 0;
      for(Map.Entry<Key,Value> theENtry : scanner) {
        count++;
        System.out.println(theENtry);
      }
      System.out.println(count);

        assertEquals(1, Iterables.size(scanner));

       assertEquals("c", Iterables.get(scanner, 0).getKey().getColumnFamily().toString());
    }




}
