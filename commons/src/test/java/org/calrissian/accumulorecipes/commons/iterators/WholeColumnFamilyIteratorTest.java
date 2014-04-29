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

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WholeColumnFamilyIteratorTest {

    Connector connector;

    @Before
    public void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException {
        Instance instance = new MockInstance();
        connector = instance.getConnector("user", "".getBytes());
        connector.tableOperations().create("test");
    }

    @Test
    public void testWholeColumnBatched_singleRow() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

        persistTestMutations(1, 500);

        Scanner scanner = buildScanner(10);

        assertEquals(1, Iterables.size(scanner));
        for(Map.Entry<Key,Value> entry : scanner) {
            Map<Key,Value> items = WholeColumnFamilyIterator.decodeRow(entry.getKey(), entry.getValue());
            assertEquals(500, Iterables.size(items.entrySet()));
        }
    }

    @Test
    public void testWholeColumnBatched_multipleRows() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

        persistTestMutations(50, 500);

        Scanner scanner = buildScanner(10);

        assertEquals(50, Iterables.size(scanner));
        for(Map.Entry<Key,Value> entry : scanner) {
            Map<Key,Value> items = WholeColumnFamilyIterator.decodeRow(entry.getKey(), entry.getValue());
            assertEquals(500, Iterables.size(items.entrySet()));
        }
    }


    private void persistTestMutations(int numRows, int entriesPerRow) throws TableNotFoundException, MutationsRejectedException {

        BatchWriter writer = connector.createBatchWriter("test", 1000, 1000, 1);

        for(int j = 0; j < numRows; j++) {
            Mutation m = new Mutation(Integer.toString(j));
            for(int i = 0; i < entriesPerRow; i++)
                m.put(new Text(Integer.toString(j)), new Text(String.valueOf(i)), new Value("".getBytes()));

            writer.addMutation(m);
        }
        writer.flush();
    }

    private Scanner buildScanner(int n) throws TableNotFoundException {
        IteratorSetting setting = new IteratorSetting(5, WholeColumnFamilyIterator.class);

        Scanner scanner = connector.createScanner("test", new Authorizations());
        scanner.addScanIterator(setting);

        return scanner;
    }





}
