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
package org.calrissian.accumulorecipes.eventstore.cli;

import static org.calrissian.accumulorecipes.commons.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.junit.Assert.assertEquals;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ShardSplitterIT {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        accumuloMiniClusterDriver.deleteAllTables();
    }

    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, IOException, TableExistsException, TableNotFoundException, InterruptedException {

        Connector connector = accumuloMiniClusterDriver.getConnector();
        connector.tableOperations().create("event_shard");

        DailyShardSplitter.main(new String[] {
                accumuloMiniClusterDriver.getZooKeepers(),
                accumuloMiniClusterDriver.getInstanceName(),
            "root",
            "secret",
            "event_shard",
            "1969-01-01",
            "1969-01-01"
        });

        assertEquals(DEFAULT_PARTITION_SIZE, connector.tableOperations().listSplits("event_shard").size());

        DailyShardSplitter.main(new String[] {
                accumuloMiniClusterDriver.getZooKeepers(),
                accumuloMiniClusterDriver.getInstanceName(),
            "root",
            "secret",
            "event_shard",
            "1969-01-01",
            "1969-01-02"
        });

        System.out.println(connector.tableOperations().getSplits("event_shard"));

        assertEquals(DEFAULT_PARTITION_SIZE, connector.tableOperations().getSplits("event_shard").size());

    }
}
