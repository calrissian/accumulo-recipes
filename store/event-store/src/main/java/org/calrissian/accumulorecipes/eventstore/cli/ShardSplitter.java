package org.calrissian.accumulorecipes.eventstore.cli;


import org.apache.accumulo.core.client.*;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.eventstore.support.Constants;
import org.calrissian.accumulorecipes.eventstore.support.ShardBuilder;
import org.joda.time.DateTime;
import sun.security.provider.SHA;

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

        SortedSet<Text> shards = new ShardBuilder(Constants.DEFAULT_PARTITION_SIZE)
                .buildShardsInRange(start.toDate(), stop.toDate());

        connector.tableOperations().addSplits(tableName, shards);
    }
}
