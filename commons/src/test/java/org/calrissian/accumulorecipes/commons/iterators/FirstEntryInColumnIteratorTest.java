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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FirstEntryInColumnIteratorTest {

    Connector connector;

    @Before
    public void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException {
        Instance instance = new MockInstance();
        connector = instance.getConnector("user", "".getBytes());
        connector.tableOperations().create("test");
    }

    @Test
    public void testFirstEntryReturned_singleRowLotsOfColumns() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

        persistTestMutations(1, 4, 4);

        Scanner scanner = buildScanner();
        assertEquals(4, Iterables.size(scanner));
    }

    @Test
    public void testFirstEntryReturned_multipleRowsLotsOfColumns() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

        persistTestMutations(4, 4, 4);

        Scanner scanner = buildScanner();
        assertEquals(16, Iterables.size(scanner));
    }

    private void persistTestMutations(int numRows, int entriesPerRow, int numColQs) throws TableNotFoundException, MutationsRejectedException {

        BatchWriter writer = connector.createBatchWriter("test", 1000, 1000, 1);

        for (int j = 0; j < numRows; j++) {
            Mutation m = new Mutation(Integer.toString(j));
            for (int i = 0; i < entriesPerRow; i++) {
                for(int q = 0; q < numColQs;   q++)
                    m.put(new Text(Integer.toString(i)), new Text(Integer.toString(q)), new Value("".getBytes()));
            }

            writer.addMutation(m);
        }
        writer.flush();
    }

    private Scanner buildScanner() throws TableNotFoundException {
        IteratorSetting setting = new IteratorSetting(5, FirstEntryInColumnIterator.class);
        Scanner scanner = connector.createScanner("test", new Authorizations());
        scanner.addScanIterator(setting);

        return scanner;
    }


}
