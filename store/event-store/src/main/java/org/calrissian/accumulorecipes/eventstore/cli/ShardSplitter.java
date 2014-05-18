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


import org.apache.accumulo.core.client.*;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.joda.time.DateTime;

import java.util.SortedSet;

public class ShardSplitter {

    public static void main(String args[]) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        if(args.length != 7) {
            System.out.println("Usage: " + ShardSplitter.class.getName() + "<zookeepers> <instance> <username> <password> <tableName> <start day: yyyy-mm-dd> <stop day: yyyy-mm-dd>");
            System.exit(1);
        }

        String zookeepers = args[0];
        String instance = args[1];
        String username = args[2];
        String password = args[3];
        String tableName = args[4];
        DateTime start = DateTime.parse(args[5]);
        DateTime stop = DateTime.parse(args[6]);


        Instance accInst = new ZooKeeperInstance(instance, zookeepers);
        Connector connector = accInst.getConnector(username, password.getBytes());

        SortedSet<Text> shards = new HourlyShardBuilder(Constants.DEFAULT_PARTITION_SIZE)
                .buildShardsInRange(start.toDate(), stop.toDate());

        connector.tableOperations().addSplits(tableName, shards);
    }
}
