/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.iterators;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FirstEntryInPrefixedRowTest {

  Connector connector;

  @Before
  public void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException {
    Instance instance = new MockInstance();
    connector = instance.getConnector("user", "".getBytes());
    connector.tableOperations().create("test");
  }

  @Test
  public void testFirstEntryReturned_singleRowLotsOfColumns() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

    persistTestMutations(50);

    Scanner scanner = buildScanner();
    for(Map.Entry<Key,Value> entry : scanner)
      System.out.println(entry);
    assertEquals(50, Iterables.size(scanner));
  }


  private void persistTestMutations(int numRows) throws TableNotFoundException, MutationsRejectedException {

    BatchWriter writer = connector.createBatchWriter("test", 1000, 1000, 1);

    for (int j = 0; j < numRows; j++) {
      Mutation m = new Mutation(Integer.toString(j) + "|suffix");
      m.put(new Text(), new Text(), new Value("".getBytes()));

      for(int i = 0; i < numRows; i++)
        m.put(new Text("" +i), new Text(), new Value("".getBytes()));
      writer.addMutation(m);
    }
    writer.flush();
  }

  private Scanner buildScanner() throws TableNotFoundException {
    IteratorSetting setting = new IteratorSetting(5, TestFirstEntryInPrefixedRowIterator.class);
    Scanner scanner = connector.createScanner("test", new Authorizations());
    scanner.addScanIterator(setting);

    return scanner;
  }

  public static class TestFirstEntryInPrefixedRowIterator extends FirstEntryInPrefixedRowIterator {

    @Override protected String getPrefix(String rowStr) {

      int lastPipeIfx = rowStr.lastIndexOf("|");
      return rowStr.substring(0, lastPipeIfx);
    }
  }
}
